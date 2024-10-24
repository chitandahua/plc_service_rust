use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, Condvar, Mutex};
use std::time::Duration;

use crate::protocol::Address;
use crate::request_info::UartMessage;
use crate::{ModuleService, Result};

use super::{MeterState, ModuleInfo};

enum InitEvent {
    Result(Result<()>),
    Address(Address),
}

#[derive(Default)]
struct PlcInitResult {
    event: Option<InitEvent>,
}

pub struct PlcInit {
    uart_msg_sender: mpsc::Sender<UartMessage>,
    services: ModuleService,
    init_result: Mutex<PlcInitResult>,
    cond: Condvar,
    init_timeout: Duration,
    init_flag: Arc<AtomicBool>,
}

impl PlcInit {
    pub fn new(
        uart_msg_sender: mpsc::Sender<UartMessage>,
        services: ModuleService,
        init_timeout: u32,
    ) -> Self {
        Self {
            uart_msg_sender,
            services,
            init_result: Mutex::new(PlcInitResult::default()),
            cond: Condvar::new(),
            init_timeout: Duration::from_secs(init_timeout as u64),
            init_flag: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn initailized(&self) -> bool {
        self.init_flag.load(Ordering::Relaxed)
    }

    fn notify_event(&self, event: InitEvent) {
        let mut res = self.init_result.lock().unwrap();
        res.event = Some(event);
        self.cond.notify_one();
    }

    pub fn notify(&self, result: Result<()>) {
        self.notify_event(InitEvent::Result(result));
    }

    pub fn update_address(&self, address: Address) {
        self.notify_event(InitEvent::Address(address));
    }

    pub fn notify_timeout(&self) {
        self.notify_event(InitEvent::Result(Err(PlcInitError::Timeout.into())));
    }

    fn wait_for_event_timeout(&self, timeout: Duration) -> Result<InitEvent> {
        let mut res = self.init_result.lock().unwrap();
        let result = self
            .cond
            .wait_timeout_while(res, timeout, |r| r.event.is_none())
            .unwrap();
        res = result.0;
        if result.1.timed_out() {
            return Err(PlcInitError::Timeout.into());
        }

        Ok(res.event.take().unwrap())
    }

    fn wait_for_event(&self) -> Result<InitEvent> {
        let mut res = self.init_result.lock().unwrap();
        res = self.cond.wait_while(res, |r| r.event.is_none()).unwrap();

        Ok(res.event.take().unwrap())
    }

    fn wait_result(&self) -> Result<()> {
        match self.wait_for_event()? {
            InitEvent::Result(result) => result,
            _ => Err(PlcInitError::InvalidEvent.into()),
        }
    }

    fn wait_init_address(&self) -> Result<Address> {
        match self.wait_for_event_timeout(self.init_timeout)? {
            InitEvent::Address(address) => Ok(address),
            _ => Err(PlcInitError::InvalidEvent.into()),
        }
    }

    fn wait_address(&self) -> Result<Address> {
        match self.wait_for_event()? {
            InitEvent::Address(address) => Ok(address),
            _ => Err(PlcInitError::InvalidEvent.into()),
        }
    }

    pub fn get_master_address(&self) -> Result<Address> {
        let retry_count = 3;
        for i in 0..retry_count {
            ModuleInfo::get_module_info(None, &self.uart_msg_sender);
            match self.wait_address() {
                Ok(address) => {
                    return Ok(address);
                }
                Err(e) => {
                    if i == retry_count - 1 {
                        return Err(e);
                    }
                }
            }
        }

        unreachable!();
    }

    pub fn run(&self, meter_state: MeterState) -> Result<()> {
        self.init_flag.store(false, Ordering::Relaxed);

        // 等待模块信息上报
        let master_address = self.services.master_address.get_master_address();
        let address_match = match self.wait_init_address() {
            Ok(address) => master_address == address,
            Err(_) => self.get_master_address()? == master_address,
        };

        // 如果地址不匹配，重新设置
        if !address_match {
            self.services
                .master_address
                .init_set_address(master_address, &self.uart_msg_sender);
            self.wait_result()?;
        }

        // 清空档案
        self.services
            .node_manage
            .init_clear_acq_files(&self.uart_msg_sender)?;
        meter_state.init_param();

        // 加载档案
        self.services
            .node_manage
            .load_config(&self.uart_msg_sender)?;

        self.init_flag.store(true, Ordering::Relaxed);
        Ok(())
    }
}

#[derive(thiserror::Error, Debug)]
pub enum PlcInitError {
    #[error("init timeout")]
    Timeout,
    #[error("unexpected event type")]
    InvalidEvent,
}
