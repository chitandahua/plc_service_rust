use std::sync::{mpsc, Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use tracing::{debug, error, warn};

use crate::{SerialPort, Result};
use crate::{Frame, ReqInfo, UartMessage};

//use crate::serial_port::ConnectionError;

#[derive(Debug)]
pub struct UartAgent {
    uart_requeset_receiver: mpsc::Receiver<UartMessage>,
    req_info: Arc<Mutex<Option<ReqInfo>>>,
    cond: Arc<Condvar>,
}

// TODO
// UartHandler

impl UartAgent {
    pub fn new(
        uart_requeset_receiver: mpsc::Receiver<UartMessage>,
    ) -> Self {
        UartAgent {
            uart_requeset_receiver,
            req_info: Arc::new(Mutex::new(None)),
            cond: Arc::new(Condvar::new()),
        }
    }

    pub fn run(self, uart_config: PathBuf, tcp_addr: Option<SockAddr>, handler: impl UartHandler + Send + 'static) -> Result<Vec<JoinHandle<()>>> {
        let UartAgent {
            uart_requeset_receiver,
            req_info,
            cond
        } = self;

        let SerialPort {
            mut reader,
            mut writer,
        } = SerialPort::new(uart_config, tcp_addr)?;

        let req_info_clone = req_info.clone();
        let cond_clone = cond.clone();

        let uart_agent_thread = thread::spawn(move || {
            const MAX_RETRY: usize = 1; // 不重试
            while let Ok(req_msg) = uart_requeset_receiver.recv() {
                let UartMessage {
                    req_info,
                    frame,
                } = req_msg;
                debug!("recv frame {}", frame);

                let mut cnt = 0;
                {
                    let mut msg = req_info.lock().unwrap();
                    msg = Some(req_info);

                    while cnt < MAX_RETRY {
                        match writer.write_request(frame) {
                            Ok(_) => {}
                            Err(err) => {
                                warn!("write error {}", err);
                            }
                        };

                        let result = cond.wait_timeout(msg, Duration::from_millis(200), |&mut req_info| req_info.is_some()).unwrap();
                        msg = result.0;
                        if !result.1.timed_out() {
                            break;
                        }
                        cnt += 1;
                    }
                    
                    // 超时处理 TODO
                    if msg.is_some() {
                        warning!("timeout");
                    }
                }
                
            }

            debug!("uart_agent_thread finish");
        });

        let uart_handler_thread = thread::spawn(move || loop {
            match reader.read_response() {
                Ok(Some(response)) => {
                    debug!("uart response {}", response);

                    let req_info;
                    let invalid = {
                        // 根据response获取cmd
                        let mut msg = req_info_clone.lock().unwrap();
                        req_info = msg.take();
                        let result = if response.is_report() {
                            true
                        } else if req_info.is_some() && response.match_req(req_info) {
                            // 调用对应处理函数 TODO 锁外调用
                            true
                        } else {
                            // no request or not match TODO
                            warn!("invalid response");
                            false
                        }
                        
                        cond_clone.notify_one();
                        result
                    };

                    if !invalid {
                        continue;
                    }

                    match handler.uart_msg_handler(UartMessage::new(req_info, response)) {
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
