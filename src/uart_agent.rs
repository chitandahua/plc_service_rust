use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{mpsc, Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use timer::{Guard, Timer};
use tracing::{debug, error, info, warn};

use crate::mqtt_message::MqttMessage;
use crate::serial_port::{StreamReader, StreamWriter};
use crate::{ReqInfo, UartMessage};
use crate::{Result, UartPort};

#[derive(Debug)]
struct UartConfig {
    timeout: Duration,
    concurrent_timeout: chrono::Duration,
}

struct UartReqInfo {
    req_info: Option<ReqInfo>,
    concurrent_req_info: HashMap<u8, (ReqInfo, Guard)>,
    writer: StreamWriter,
}

pub struct UartAgent {
    mqtt_msg_sender: mpsc::Sender<MqttMessage>,
    uart_requeset_receiver: mpsc::Receiver<UartMessage>,
    concurrent_req_receiver: mpsc::Receiver<UartMessage>,
    cur_req_info: Arc<Mutex<UartReqInfo>>,
    cond: Arc<Condvar>,
    reader: StreamReader,
    config: UartConfig,
}

pub trait UartHandler {
    fn uart_msg_handler(&mut self, message: UartMessage) -> Result<()>;
}

impl UartAgent {
    pub fn new(
        mqtt_msg_sender: mpsc::Sender<MqttMessage>,
        uart_requeset_receiver: mpsc::Receiver<UartMessage>,
        concurrent_req_receiver: mpsc::Receiver<UartMessage>,
        uart_config: PathBuf,
        tcp_addr: Option<SocketAddr>,
    ) -> Result<Self> {
        let UartPort {
            mut reader,
            mut writer,
        } = UartPort::new(uart_config, tcp_addr)?;

        Ok(UartAgent {
            mqtt_msg_sender,
            uart_requeset_receiver,
            concurrent_req_receiver,
            cur_req_info: Arc::new(Mutex::new(UartReqInfo {
                req_info: None,
                concurrent_req_info: HashMap::new(),
                writer,
            })),
            cond: Arc::new(Condvar::new()),
            reader,
            config: UartConfig {
                timeout: Duration::from_millis(6000),
                concurrent_timeout: chrono::Duration::seconds(60),
            },
        })
    }

    pub fn run(
        self,
        mut handler: impl UartHandler + Send + 'static,
        timer: Arc<Timer>,
    ) -> Result<Vec<JoinHandle<()>>> {
        debug!("uart_agent start");
        let UartAgent {
            mqtt_msg_sender,
            uart_requeset_receiver,
            concurrent_req_receiver,
            cur_req_info,
            mut reader,
            cond,
            config,
        } = self;

        let mqtt_msg_sender_clone = mqtt_msg_sender.clone();
        let cur_req_info_clone = cur_req_info.clone();
        let concurrent_req_info = cur_req_info.clone();
        let cond_clone = cond.clone();

        let uart_agent_thread = thread::spawn(move || {
            const MAX_RETRY: usize = 1; // 不重试
            while let Ok(req_msg) = uart_requeset_receiver.recv() {
                let UartMessage { req_info, frame } = req_msg;
                debug!("recv request frame {}", frame.to_hex_string());

                let mut cnt = 0;
                {
                    let bytes = Into::<Vec<u8>>::into(frame);
                    let mut lock = cur_req_info.lock().unwrap();
                    lock.req_info = Some(req_info);

                    while cnt < MAX_RETRY {
                        match lock.writer.write_request(&bytes) {
                            Ok(_) => {}
                            Err(err) => {
                                warn!("write error {}", err);
                            }
                        };

                        let result = cond
                            .wait_timeout_while(lock, config.timeout, |req| req.req_info.is_some())
                            .unwrap();
                        lock = result.0;
                        if !result.1.timed_out() {
                            break;
                        }
                        cnt += 1;
                    }

                    // 超时处理
                    let req = lock.req_info.take();
                    if let Some(mut req) = req {
                        warn!("request seq {} timeout", req.seq_num());
                        let cb = req.timeout_cb.take();
                        if let Some(cb) = cb {
                            cb(req.into_mqtt_req_info().unwrap(), mqtt_msg_sender.clone());
                        }
                    }
                }
            }

            info!("uart_agent_thread finish");
        });

        let uart_concurrent_thread = thread::spawn(move || loop {
            while let Ok(req_msg) = concurrent_req_receiver.recv() {
                let UartMessage {
                    mut req_info,
                    frame,
                } = req_msg;
                debug!("recv concurrent request frame {}", frame.to_hex_string());

                let seq = frame.get_seq();
                let bytes = Into::<Vec<u8>>::into(frame);
                let timeout_cb = req_info.timeout_cb.take();
                let concurrent_req_info_clone = concurrent_req_info.clone();
                let mqtt_msg_sender = mqtt_msg_sender_clone.clone();
                let guard = timer.schedule_with_delay(config.concurrent_timeout, move || {
                    let mqtt_req_info = {
                        let mut lock = concurrent_req_info_clone.lock().unwrap();
                        // 若超时 且当前只有mqtt会并行请求 故全unwrap
                        lock.concurrent_req_info
                            .remove(&seq)
                            .unwrap()
                            .0
                            .into_mqtt_req_info()
                            .unwrap()
                    };
                    // 其实也可以unwrap
                    if let Some(cb) = &timeout_cb {
                        cb(mqtt_req_info, mqtt_msg_sender.clone());
                    }
                });
                {
                    let mut lock = concurrent_req_info.lock().unwrap();
                    let res = lock.writer.write_request(&bytes);
                    match res {
                        Ok(_) => {
                            lock.concurrent_req_info.insert(seq, (req_info, guard));
                        }
                        Err(error) => error!("write error {}", error),
                    }
                }
            }
        });

        let uart_handler_thread = thread::spawn(move || loop {
            match reader.read_response() {
                Ok(Some(response)) => {
                    debug!("uart response {}", hex::encode(response.to_bytes()));

                    let req_info = {
                        // 根据response获取cmd
                        let mut is_serial_req = false;
                        let mut lock = cur_req_info_clone.lock().unwrap();
                        let info = if response.is_slave_report() {
                            Some(ReqInfo::new(&response, None, None))
                        } else if let Some(concurrent_req) =
                            lock.concurrent_req_info.remove(&response.get_seq())
                        {
                            Some(concurrent_req.0)
                        }
                        // else if let Some(req) = lock.req_info.take()
                        //   && response.match_req(req.seq_num())
                        else if lock.req_info.is_some()
                            && response.match_req(lock.req_info.as_ref().unwrap().seq_num())
                        {
                            is_serial_req = true;
                            // 调用对应处理函数 锁外调用
                            lock.req_info.take()
                        } else {
                            // no request or not match
                            warn!("invalid response");
                            None
                        };

                        if is_serial_req {
                            cond_clone.notify_one();
                        }
                        info
                    };

                    if req_info.is_none() {
                        continue;
                    }

                    match handler.uart_msg_handler(UartMessage::new(req_info.unwrap(), response)) {
                        Ok(_) => {}
                        Err(e) => {
                            error!(casue = ?e, "handle uart response error");
                            continue;
                        }
                    }
                }
                Ok(None) => continue,
                Err(err) => {
                    error!("read uart response error: {:?}", err);
                    break;
                }
            }
        });

        Ok(vec![
            uart_agent_thread,
            uart_handler_thread,
            uart_concurrent_thread,
        ])
    }
}
