use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{mpsc, Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use timer::{Guard, Timer};
use tracing::{debug, error, info, warn};

use crate::serial_port::{StreamReader, StreamWriter};
use crate::uart_handler::UartTimeoutHandler;
use crate::{ReqInfo, UartMessage};
use crate::{Result, UartConfig, UartPort};

#[derive(Debug)]
struct UartTimeout {
    timeout: Duration,
    concurrent_timeout: chrono::Duration,
}

struct UartReqInfo {
    req_info: Option<ReqInfo>,
    concurrent_req_info: HashMap<u8, (ReqInfo, Guard)>,
    writer: StreamWriter,
}

pub struct UartAgent {
    cur_req_info: Arc<Mutex<UartReqInfo>>,
    cond: Arc<Condvar>,
    reader: StreamReader,
    config: UartTimeout,
}

pub trait UartHandler {
    fn uart_msg_handler(&mut self, message: UartMessage) -> Result<()>;
}

impl UartAgent {
    pub fn new(
        uart_config: UartConfig,
        tcp_addr: Option<SocketAddr>,
        uart_timeout: u32,
        concurrent_timeout: u32,
    ) -> Result<Self> {
        let UartPort { reader, writer } = UartPort::new(uart_config, tcp_addr)?;

        Ok(UartAgent {
            cur_req_info: Arc::new(Mutex::new(UartReqInfo {
                req_info: None,
                concurrent_req_info: HashMap::new(),
                writer,
            })),
            cond: Arc::new(Condvar::new()),
            reader,
            config: UartTimeout {
                timeout: Duration::from_millis(uart_timeout as u64),
                concurrent_timeout: chrono::Duration::seconds(concurrent_timeout as i64),
            },
        })
    }

    pub fn run(
        self,
        uart_requeset_receiver: mpsc::Receiver<UartMessage>,
        concurrent_req_receiver: mpsc::Receiver<UartMessage>,
        mut handler: impl UartHandler + Send + 'static,
        timer: Arc<Timer>,
        uart_timeout_handler: UartTimeoutHandler,
        consecutive_timeouts: Arc<AtomicU8>,
    ) -> Result<Vec<JoinHandle<()>>> {
        debug!("uart_agent start");
        let UartAgent {
            cur_req_info,
            mut reader,
            cond,
            config,
        } = self;

        let uart_agent_thread = thread::spawn({
            let cond = cond.clone();
            let cur_req_info = cur_req_info.clone();
            let uart_timeout_handler = uart_timeout_handler.clone();
            move || {
                const MAX_RETRY: usize = 1; // 不重试
                while let Ok(req_msg) = uart_requeset_receiver.recv() {
                    let UartMessage { req_info, frame } = req_msg;
                    let is_response = frame.is_master_response();
                    debug!(
                        "recv {} frame {}",
                        match is_response {
                            true => "response",
                            false => "request",
                        },
                        frame.to_hex_string()
                    );

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

                            if is_response {
                                break;
                            }

                            let result = cond
                                .wait_timeout_while(lock, config.timeout, |req| {
                                    req.req_info.is_some()
                                })
                                .unwrap();
                            lock = result.0;
                            if !result.1.timed_out() {
                                break;
                            }
                            cnt += 1;
                        }

                        if is_response {
                            continue;
                        }

                        // 超时处理
                        let req = lock.req_info.take();
                        if let Some(req) = req {
                            warn!("request seq {} timeout", req.seq_num());
                            let _ = uart_timeout_handler.handle_timeout(req);
                            consecutive_timeouts.fetch_add(1, Ordering::Relaxed);
                        } else {
                            consecutive_timeouts.store(0, Ordering::Relaxed);
                        }
                    }
                }

                info!("uart_agent_thread finish");
            }
        });

        let uart_concurrent_thread = thread::spawn({
            let cur_req_info = cur_req_info.clone();
            move || loop {
                while let Ok(req_msg) = concurrent_req_receiver.recv() {
                    let UartMessage { req_info, frame } = req_msg;
                    debug!("recv concurrent request frame {}", frame.to_hex_string());

                    let seq = frame.get_seq();
                    let bytes = Into::<Vec<u8>>::into(frame);
                    let guard = timer.schedule_with_delay(config.concurrent_timeout, {
                        let cur_req_info = cur_req_info.clone();
                        let uart_timeout_handler = uart_timeout_handler.clone();
                        move || {
                            warn!("concurrent request seq {} timeout", seq);
                            let req_info = {
                                let mut lock = cur_req_info.lock().unwrap();
                                // 若超时 且当前只有mqtt会并行请求 故unwrap
                                lock.concurrent_req_info.remove(&seq).unwrap().0
                            };
                            let _ = uart_timeout_handler.handle_timeout(req_info);
                        }
                    });
                    {
                        let mut lock = cur_req_info.lock().unwrap();
                        let res = lock.writer.write_request(&bytes);
                        match res {
                            Ok(_) => {
                                lock.concurrent_req_info.insert(seq, (req_info, guard));
                            }
                            Err(error) => error!("write error {}", error),
                        }
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
                        let mut lock = cur_req_info.lock().unwrap();
                        let info = if response.is_slave_report() {
                            Some(ReqInfo::new(&response, None))
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
                            cond.notify_one();
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
