use crate::{MqttMessage, Result, APP_NAME};
use serde_json::{json, Value};
use std::sync::{mpsc, Arc, Condvar, Mutex};
use std::time::Duration;

#[derive(Clone)]
pub struct DeviceInfo {
    esn: Arc<Mutex<String>>,
    cond: Arc<Condvar>,
}

impl DeviceInfo {
    pub fn new() -> Self {
        Self {
            esn: Arc::new(Mutex::new("".to_string())),
            cond: Arc::new(Condvar::new()),
        }
    }

    fn device_info_message(topic: &str) -> MqttMessage {
        let payload = json!([]);
        MqttMessage::new(topic, payload)
    }

    pub fn run(&self, mqtt_msg_sender: &mpsc::Sender<MqttMessage>) -> Result<()> {
        const RETRY_COUNT: usize = 60;
        const RETRY_INTERVAL_MS: u64 = 1000;
        let topic = format!("{}{}", APP_NAME, "/get/request/osmanage/deviceInfo");
        let mut count = 0;

        {
            let mut esn = self.esn.lock().unwrap();
            while count < RETRY_COUNT {
                let msg = Self::device_info_message(&topic);
                let _ = mqtt_msg_sender.send(msg)?;

                let result = self
                    .cond
                    .wait_timeout_while(esn, Duration::from_millis(RETRY_INTERVAL_MS), |esn| {
                        esn.is_empty()
                    })
                    .unwrap();
                esn = result.0;
                if !result.1.timed_out() {
                    break;
                }

                count += 1;
            }
        }

        anyhow::ensure!(count < RETRY_COUNT, "get device info failed");
        Ok(())
    }
}
