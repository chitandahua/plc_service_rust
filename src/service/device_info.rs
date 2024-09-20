use crate::mqtt_handler::MqttTopicType;
use crate::mqtt_message::PayloadBody;
use crate::{MqttMessage, MqttMsgHandler, MqttPayload, Result, APP_NAME};
use serde::Deserialize;
use serde_json::json;
use std::sync::{mpsc, Arc, Condvar, Mutex};
use std::time::Duration;

#[derive(Clone)]
pub struct DeviceInfo {
    esn: Arc<Mutex<String>>,
    cond: Arc<Condvar>,
}

#[derive(Debug, Deserialize)]
struct DeviceInfoResponse {
    #[serde(rename = "deviceESN")]
    device_esn: String,
}

impl DeviceInfo {
    pub fn new() -> Self {
        Self {
            esn: Arc::new(Mutex::new("".to_string())),
            cond: Arc::new(Condvar::new()),
        }
    }

    pub fn esn(&self) -> String {
        let esn = self.esn.lock().unwrap();
        esn.clone()
    }

    fn device_info_message(topic: &str) -> MqttMessage {
        let payload = MqttPayload::new_with_body(Some(PayloadBody::Nested { body: json!([]) }));
        MqttMessage::new(topic, payload)
    }

    pub fn init(&self, mqtt_msg_handler: &mut MqttMsgHandler) {
        use crate::config::SCHEMA_PATH;
        use crate::schema_check;
        let topic = format!("{}{}{}", "osmanage/get/response/", APP_NAME, "/deviceInfo");
        let schema = schema_check::parse_schema(SCHEMA_PATH.join("device_info_schema.json")).ok();
        mqtt_msg_handler.add_topic_filter(topic, MqttTopicType::DeviceInfo, schema);
    }

    pub fn mqtt_device_info_response(&self, message: MqttMessage) {
        let response: DeviceInfoResponse = serde_json::from_str(message.payload()).unwrap();
        let mut esn = self.esn.lock().unwrap();
        *esn = response.device_esn;
        self.cond.notify_one();
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
                mqtt_msg_sender.send(msg).unwrap();

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
