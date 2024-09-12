use nix::ioctl_read;
use nix::ioctl_write_int;
use num_enum::TryFromPrimitive;
use std::fs::File;
use std::os::unix::io::AsRawFd;
use std::path::PathBuf;
use std::sync::{mpsc, Arc};
use std::time::Duration;
use std::{
    sync::atomic::{AtomicBool, AtomicU8, Ordering},
    thread::{self, JoinHandle},
};
use tracing::{debug, info, warn};

use crate::mqtt_message::{MqttMessage, MqttPayload, Status};
use crate::APP_NAME;
use crate::{PlcInit, Result};

trait State: Send {
    fn execute(&self, device: &PlcDevice) -> Box<dyn State>;
}

struct Init;
struct CheckMid;
struct Running;
struct Destroy;

impl State for Init {
    fn execute(&self, device: &PlcDevice) -> Box<dyn State> {
        info!("Executing Init state");
        match device.plc_power_on() {
            Ok(_) => Box::new(CheckMid),
            Err(err) => {
                warn!(cause = ?err, "plc device init error");
                Box::new(Init)
            }
        }
    }
}

impl State for CheckMid {
    fn execute(&self, device: &PlcDevice) -> Box<dyn State> {
        info!("Executing CheckMid state");
        // 重试3次
        let retry_cnt = 3;
        for _ in 0..retry_cnt {
            match device.plc_device_on() {
                Ok(status) if status => {
                    device
                        .online
                        .store(true, std::sync::atomic::Ordering::Relaxed);
                    device.model_plugin_notify(true);
                    return Box::new(Running);
                }
                _ => thread::sleep(Duration::from_secs(3)),
            }
        }

        Box::new(Destroy)
    }
}

impl State for Running {
    fn execute(&self, device: &PlcDevice) -> Box<dyn State> {
        info!("Executing Running state");
        match device.plc_device_run() {
            Ok(_) => Box::new(Running),
            Err(err) => {
                warn!(cause = ?err, "plc device run error");
                Box::new(Destroy)
            }
        }
    }
}

impl State for Destroy {
    fn execute(&self, device: &PlcDevice) -> Box<dyn State> {
        info!("Executing Destroy state");
        match device.plc_power_off() {
            Ok(_) => Box::new(Init),
            Err(_) => Box::new(Destroy),
        }
    }
}

#[derive(Clone)]
pub struct PlcDevice {
    port: PathBuf,
    mqtt_msg_sender: mpsc::Sender<MqttMessage>,
    online: Arc<AtomicBool>,
    plc_init: Arc<PlcInit>,
    consecutive_timeouts: Arc<AtomicU8>,
}

#[derive(Debug, PartialEq, TryFromPrimitive)]
#[repr(i32)]
enum PinStatus {
    Low = 0,
    High = 1,
}

const DEV_IOC_MAGIC: u8 = b'P';

ioctl_write_int!(rstw_iocmode, DEV_IOC_MAGIC, 0);
ioctl_write_int!(powerw_iocmode, DEV_IOC_MAGIC, 4);
ioctl_read!(midr_iocmode, DEV_IOC_MAGIC, 6, i32);

const LEVEL_DURATION: u64 = 200; // ms
const CHECK_DURATION: u64 = 3000; // ms
const CHECK_SECONDS: u64 = 600; // s

const ON: u64 = 0x01;
const OFF: u64 = 0x00;

impl PlcDevice {
    pub fn new(
        port: PathBuf,
        mqtt_msg_sender: mpsc::Sender<MqttMessage>,
        plc_init: Arc<PlcInit>,
        consecutive_timeouts: Arc<AtomicU8>,
    ) -> Self {
        PlcDevice {
            port,
            mqtt_msg_sender,
            online: Arc::new(AtomicBool::new(false)),
            plc_init,
            consecutive_timeouts,
        }
    }

    pub fn run(self) -> crate::Result<JoinHandle<()>> {
        let handler = thread::spawn(move || {
            let mut state: Box<dyn State> = Box::new(Init);
            loop {
                state = state.execute(&self);
                thread::sleep(Duration::from_millis(300));
            }
        });

        Ok(handler)
    }

    pub fn available(&self) -> bool {
        //self.online.load(Ordering::Relaxed) && self.plc_init.initailized()
        true
    }

    fn plc_power_on(&self) -> Result<()> {
        let file = File::open(&self.port)?;
        let fd = file.as_raw_fd();

        // 下电然后上电
        unsafe {
            powerw_iocmode(fd, OFF)?;
            thread::sleep(Duration::from_millis(2000));
            powerw_iocmode(fd, ON)?;
            thread::sleep(Duration::from_millis(2000));
        }

        // 复位脚拉低 => 拉高

        unsafe {
            rstw_iocmode(fd, OFF)?;
            thread::sleep(Duration::from_millis(500));
            rstw_iocmode(fd, ON)?;
            thread::sleep(Duration::from_millis(2000));
        }

        Ok(())
    }

    fn plc_device_on(&self) -> Result<bool> {
        self.plc_device_check_status(PinStatus::Low)
    }

    fn plc_device_check_status(&self, status: PinStatus) -> Result<bool> {
        if self.plc_mid_check()? == status {
            thread::sleep(Duration::from_millis(LEVEL_DURATION));
            Ok(self.plc_mid_check()? == status)
        } else {
            Ok(false)
        }
    }

    fn plc_mid_check(&self) -> Result<PinStatus> {
        let file = File::open(&self.port)?;
        let fd = file.as_raw_fd();

        let mut result: i32 = 0;
        unsafe {
            midr_iocmode(fd, &mut result)?;
        }

        Ok(PinStatus::try_from(result)?)
    }

    fn plc_power_off(&self) -> Result<()> {
        let file = File::open(&self.port)?;
        let fd = file.as_raw_fd();

        let ops = [(powerw_iocmode, OFF)];
        let intervals = [2000];

        for (&(op, arg), &interval) in ops.iter().zip(intervals.iter()) {
            unsafe {
                op(fd, arg)?;
            }
            std::thread::sleep(Duration::from_millis(interval));
        }

        Ok(())
    }

    fn model_plugin_notify(&self, online: bool) {
        let topic = APP_NAME.to_string() + "/notify/spont/*/modePlugin";
        let (status, reason) = match online {
            true => (Status::Success, "module plugin"),
            false => (Status::Failure, "module pullout"),
        };
        let payload = MqttPayload::new_with_status_reason(status, reason);
        let msg = MqttMessage::new(topic, payload);
        self.mqtt_msg_sender.send(msg).unwrap();
    }

    fn plc_device_run(&self) -> Result<()> {
        if self.plc_init.initailized() {
            let times = CHECK_SECONDS / (CHECK_DURATION / 1000);
            for i in 0..times {
                thread::sleep(Duration::from_millis(CHECK_DURATION - LEVEL_DURATION));
                if !self.plc_device_on()? {
                    anyhow::bail!("plc device off");
                }

                if i % 20 == 0 && self.consecutive_timeouts.load(Ordering::Relaxed) >= 5 {
                    anyhow::bail!("plc device timeout more than 5 times");
                }
            }

            debug!("get module info per {} seconds", CHECK_SECONDS);
            self.plc_init.get_master_address()?;
        } else {
            self.plc_init.run()?;
        }

        Ok(())
    }
}
