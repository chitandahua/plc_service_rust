use std::sync::{mpsc, Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use tracing::{debug, error, warn};

use crate::{SerialPort, Result};
use crate::{Frame, MessageHeader, UartMessage};

//use crate::serial_port::ConnectionError;

#[derive(Debug)]
pub struct UartAgent {
    uart_requeset_receiver: mpsc::Receiver<UartMessage>,
}

#[derive(Debug)]
struct UartMessage {

    header: Option<MessageHeader>,
}

impl UartAgent {
    pub fn new(
        uart_requeset_receiver: mpsc::Receiver<UartMessage>,
    ) -> Self {
        UartAgent {
            uart_requeset_receiver,
        }
    }

    pub fn run(self, uart_config: PathBuf, tcp_addr: Option<SockAddr>) -> Result<Vec<JoinHandle<()>>> {
        let UartAgent {
            uart_requeset_receiver,
        } = self;

        let SerialPort {
            mut reader,
            mut writer,
        } = SerialPort::new(uart_config, tcp_addr)?;

        let mutex = Arc::new(Mutex::new(UartMessage {
            current_cmd: AtCmd::AtNone,
            status: AtStatus::ResetInit,
            header: None,
        }));
        let cond = Arc::new(Condvar::new());
        let mutex_clone = mutex.clone();
        let cond_clone = cond.clone();

        let uart_agent_thread = thread::spawn(move || {
            const MAX_RETRY: usize = 5;
            while let Ok((header, cmd)) = uart_requeset_receiver.recv() {
                debug!("recv at cmd {}", cmd);

                let mut cnt = 0;
                let mut msg = mutex.lock().unwrap();
                let at_cmd = cmd.to_at_cmd();
                msg.current_cmd = cmd;
                msg.header = header;

                while cnt < MAX_RETRY {
                    match writer.write_request(&at_cmd[..]) {
                        Ok(_) => {}
                        Err(err) => {
                            warn!("write error {}", err);
                        }
                    };

                    let result = cond.wait_timeout(msg, Duration::from_millis(200)).unwrap();
                    msg = result.0;
                    if msg.current_cmd == AtCmd::AtNone {
                        break;
                    }
                    cnt += 1;
                }
                msg.current_cmd = AtCmd::AtNone;
                if cnt >= MAX_RETRY
                    && (msg.status == AtStatus::ResetInit || msg.status == AtStatus::Init)
                {
                    //panic!("AT command init error!!!");
                    error!("AT command init error!!!");
                    std::process::exit(1);
                }

                // drop(msg);
                // 构造错误回复 TODO
                // 不同At指令类型 需要构造不同的回复
            }

            debug!("uart_agent_thread finish");
        });

        let uart_response_thread = thread::spawn(move || loop {
            match reader.read_response() {
                Ok(Some(response)) => {
                    debug!("uart response {}", response);

                    // 根据response获取cmd
                    let mut msg = mutex_clone.lock().unwrap();
                    match msg.current_cmd == response {
                        true => {
                            msg.current_cmd = AtCmd::AtNone;
                            msg.status = match msg.status {
                                AtStatus::ResetInit => AtStatus::Init,
                                AtStatus::Init => AtStatus::Idle,
                                _ => AtStatus::Idle,
                            };
                        }
                        false => {
                            warn!("invalid AT command response");
                            cond_clone.notify_one();
                            drop(msg);
                            continue;
                        }
                    };
                    let header = msg.header.take();
                    cond_clone.notify_one();
                    drop(msg);

                    match uart_response_sender.send((header, response)) {
                        Ok(_) => {}
                        Err(e) => {
                            error!(casue = ?e, "uart_response_sender send error");
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

        Ok(vec![uart_agent_thread, uart_response_thread])
    }
}
