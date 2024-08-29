use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{mpsc, Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use tracing::{debug, error, info, warn};

use crate::{ReqInfo, UartMessage};
use crate::{Result, UartPort};

#[derive(Debug)]
pub struct UartAgent {
    uart_requeset_receiver: mpsc::Receiver<UartMessage>,
    cur_req_info: Arc<Mutex<Option<ReqInfo>>>,
    cond: Arc<Condvar>,
}

pub trait UartHandler {
    fn uart_msg_handler(&mut self, message: UartMessage) -> Result<()>;
}

impl UartAgent {
    pub fn new(uart_requeset_receiver: mpsc::Receiver<UartMessage>) -> Self {
        UartAgent {
            uart_requeset_receiver,
            cur_req_info: Arc::new(Mutex::new(None)),
            cond: Arc::new(Condvar::new()),
        }
    }

    pub fn run(
        self,
        uart_config: PathBuf,
        tcp_addr: Option<SocketAddr>,
        mut handler: impl UartHandler + Send + 'static,
    ) -> Result<Vec<JoinHandle<()>>> {
        debug!("uart_agent start");
        let UartAgent {
            uart_requeset_receiver,
            cur_req_info,
            cond,
        } = self;

        let UartPort {
            mut reader,
            mut writer,
        } = UartPort::new(uart_config, tcp_addr)?;

        let cur_req_info_clone = cur_req_info.clone();
        let cond_clone = cond.clone();

        let uart_agent_thread = thread::spawn(move || {
            const MAX_RETRY: usize = 1; // 不重试
            while let Ok(req_msg) = uart_requeset_receiver.recv() {
                let UartMessage { req_info, frame } = req_msg;
                debug!("recv request frame {}", frame.to_hex_string());

                let mut cnt = 0;
                {
                    let bytes = Into::<Vec<u8>>::into(frame);
                    let mut msg = cur_req_info.lock().unwrap();
                    *msg = Some(req_info);

                    while cnt < MAX_RETRY {
                        match writer.write_request(&bytes) {
                            Ok(_) => {}
                            Err(err) => {
                                warn!("write error {}", err);
                            }
                        };

                        let result = cond
                            .wait_timeout_while(msg, Duration::from_millis(6000), |req| {
                                req.is_some()
                            })
                            .unwrap();
                        msg = result.0;
                        if !result.1.timed_out() {
                            break;
                        }
                        cnt += 1;
                    }

                    // 超时处理 TODO
                    if msg.is_some() {
                        warn!("timeout");
                    }
                }
            }

            info!("uart_agent_thread finish");
        });

        let uart_handler_thread = thread::spawn(move || loop {
            match reader.read_response() {
                Ok(Some(response)) => {
                    debug!("uart response {}", hex::encode(response.to_bytes()));

                    let req_info = {
                        // 根据response获取cmd
                        let mut msg = cur_req_info_clone.lock().unwrap();
                        let req = msg.take();
                        let info = if response.is_slave_report() {
                            Some(ReqInfo::new(&response, None))
                        } else if req.is_some()
                            && response.match_req(req.as_ref().unwrap().seq_num())
                        {
                            // 调用对应处理函数 锁外调用
                            req
                        } else {
                            // no request or not match
                            warn!("invalid response");
                            None
                        };

                        cond_clone.notify_one();
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

        Ok(vec![uart_agent_thread, uart_handler_thread])
    }
}
