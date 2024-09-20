use chrono::{self, DateTime};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, VecDeque};
use std::ops::Deref;
use std::sync::{mpsc, Arc, Mutex};
use thiserror::Error;
use timer::{Guard, Timer};

use crate::config::MeterReadingConfig;
use crate::mqtt_handler::MqttTopicType;
use crate::mqtt_message::{MqttPayload, PayloadBody, Status};
use crate::mqtt_topic::MqttTopic;
use crate::protocol::app_data::{Address, ConcurrentReadMeterRequest, ConcurrentReadMeterResponse};
use crate::protocol::{AddressField, Frame};
use crate::request_info::{MqttReqInfo, ReqInfo, UartMessage};
use crate::service::parse_response::uart_response_mqtt_handler;
use crate::service::IntoMqttMessage;
use crate::{MqttMessage, MqttMsgHandler, MqttResponseError, Result, APP_NAME};

struct SampleCache {
    is_waiting_response: bool,
    msg_cache_queue: VecDeque<MqttMessage>,
    last_operation_time: DateTime<chrono::Utc>,
}

impl SampleCache {
    fn new() -> Self {
        SampleCache {
            is_waiting_response: false,
            msg_cache_queue: VecDeque::new(),
            last_operation_time: chrono::Utc::now(),
        }
    }

    fn update_operation_time(&mut self) {
        self.last_operation_time = chrono::Utc::now();
    }
}

#[derive(Clone)]
pub struct ConcurrentMeter {
    concurrent_meter: Arc<ConcurrentMeterManager>,
    _aging_queue: Guard,
}

struct ConcurrentMeterManager {
    meter_config: MeterReadingConfig,
    sample_cache: Mutex<HashMap<String, SampleCache>>,
}

impl ConcurrentMeterManager {
    fn new(meter_config: MeterReadingConfig) -> Self {
        Self {
            meter_config,
            sample_cache: Mutex::new(HashMap::new()),
        }
    }
}

#[derive(Error, Debug)]
enum ConcurrentMeterError {
    #[error("concurrent addr exceed limit {0}")]
    AddrLimit(usize),
    #[error("addr queue size exceed limit {0}")]
    QueueLimit(usize),
    #[error("Meter reading failed. Cco response data empty")]
    MeterReading,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct SamplePayload {
    #[serde(rename = "taskID")]
    task_id: String,
    #[serde(rename = "acqAddr")]
    acq_addr: String,
    #[serde(rename = "proType")]
    pro_type: String,
    data: String,
}

impl IntoMqttMessage for ConcurrentReadMeterResponse {
    fn into_mqtt_message(self, mut mqtt_req_info: MqttReqInfo) -> MqttMessage {
        let mut extra_data = mqtt_req_info
            .get_extra_data()
            .unwrap()
            .downcast::<SamplePayload>()
            .unwrap();
        extra_data.data = hex::encode(self.message);
        match extra_data.data.is_empty() {
            true => MqttMessage::new_with_req_info_status_reason(
                mqtt_req_info,
                Status::Failure,
                ConcurrentMeterError::MeterReading,
            ),
            false => MqttMessage::new_with_req_info_body(
                mqtt_req_info,
                Some(PayloadBody::Flat(serde_json::to_value(extra_data).unwrap())),
            ),
        }
    }
}

impl ConcurrentMeter {
    pub fn new(timer: &Timer, meter_config: MeterReadingConfig) -> Self {
        let concurrent_meter = Arc::new(ConcurrentMeterManager::new(meter_config));

        let queue_aging_time = concurrent_meter.meter_config.queue_aging_time as i64;
        let _aging_queue = timer.schedule_repeating(chrono::Duration::minutes(queue_aging_time), {
            let concurrent_meter = concurrent_meter.clone();
            move || {
                let mut sample_cache = concurrent_meter.sample_cache.lock().unwrap();
                sample_cache.retain(|_, v| {
                    v.is_waiting_response
                        || !v.msg_cache_queue.is_empty()
                        || chrono::Utc::now().signed_duration_since(v.last_operation_time)
                            < chrono::Duration::minutes(queue_aging_time)
                });
            }
        });
        ConcurrentMeter {
            concurrent_meter,
            _aging_queue,
        }
    }

    pub fn init(&self, mqtt_msg_handler: &mut MqttMsgHandler) {
        use crate::config::SCHEMA_PATH;
        use crate::schema_check;
        let concurrent_meter_topic = format!("{}{}{}", "+/get/request/", APP_NAME, "/concurrent");
        let schema =
            schema_check::parse_schema(SCHEMA_PATH.join("concurrent_meter_schema.json")).ok();
        mqtt_msg_handler.add_topic_filter(
            concurrent_meter_topic,
            MqttTopicType::ConcurrentMeter,
            schema,
        );
    }

    fn concurrent_addr_num(sample_cache: &HashMap<String, SampleCache>) -> usize {
        sample_cache.iter().fold(0, |acc, (_, v)| {
            acc + (v.is_waiting_response || !v.msg_cache_queue.is_empty()) as usize
        })
    }

    pub fn uart_meter_reading_timeout(
        &self,
        mut mqtt_req_info: MqttReqInfo,
        master_address: Address,
        concurrent_msg_sender: &mpsc::Sender<UartMessage>,
        mqtt_msg_sender: &mpsc::Sender<MqttMessage>,
    ) -> Result<()> {
        let extra_data = mqtt_req_info
            .get_extra_data()
            .unwrap()
            .downcast::<SamplePayload>()
            .unwrap();
        let acq_addr = extra_data.acq_addr.clone();
        let body = serde_json::to_value(extra_data).unwrap();

        let payload = MqttPayload::new(
            mqtt_req_info.token(),
            Status::Failure,
            MqttResponseError::Timeout,
            Some(PayloadBody::Flat(body)),
        );

        mqtt_msg_sender
            .send(MqttMessage::new(mqtt_req_info.topic(), payload))
            .unwrap();

        self.handle_next_request(acq_addr, master_address, concurrent_msg_sender)
    }

    fn mqtt_meter_reading_handler(
        message: MqttMessage,
        master_address: Address,
        concurrent_msg_sender: &mpsc::Sender<UartMessage>,
    ) -> Result<()> {
        let sample_payload: SamplePayload = serde_json::from_str(message.payload())?;
        let response_topic = format!(
            "{}{}{}{}",
            MqttTopic::get_app(message.topic()),
            "/notify/spont/",
            APP_NAME,
            "/reportConcurrent"
        );

        let extra_data = Box::new(sample_payload.clone());
        let frame = Frame::new_request(
            Some(AddressField::new(
                master_address,
                None,
                Address::from(sample_payload.acq_addr.as_str()),
            )),
            ConcurrentReadMeterRequest::new(
                sample_payload.pro_type.parse().unwrap(),
                hex::decode(sample_payload.data).unwrap(),
            ),
        );
        let req_info = ReqInfo::new_with_mqtt(
            &frame,
            response_topic,
            message.get_token(),
            Some(extra_data),
        );

        concurrent_msg_sender.send(UartMessage::new(req_info, frame))?;
        Ok(())
    }

    fn handle_next_request(
        &self,
        acq_addr: String,
        master_address: Address,
        concurrent_msg_sender: &mpsc::Sender<UartMessage>,
    ) -> Result<()> {
        let mut sample_cache = self.concurrent_meter.sample_cache.lock().unwrap();
        if let Some(cache) = sample_cache.get_mut(&acq_addr) {
            match cache.msg_cache_queue.is_empty() {
                true => cache.is_waiting_response = false,
                false => {
                    let result = Self::mqtt_meter_reading_handler(
                        cache.msg_cache_queue.pop_front().unwrap(),
                        master_address,
                        concurrent_msg_sender,
                    );
                    match result {
                        Ok(_) => cache.is_waiting_response = true,
                        Err(e) => return Err(e),
                    }
                }
            }
        }
        Ok(())
    }

    pub fn mqtt_concurrent_meter_reading(
        &self,
        message: MqttMessage,
        master_address: Address,
        mqtt_msg_sender: &mpsc::Sender<MqttMessage>,
        concurrent_msg_sender: &mpsc::Sender<UartMessage>,
    ) -> Result<()> {
        let value = serde_json::from_str::<Value>(message.payload())?;
        let acq_addr = value["acqAddr"].as_str().unwrap();

        let topic = MqttTopic::transfer(message.topic());
        let token = message.get_token();
        let result = {
            let mut sample_cache = self.concurrent_meter.sample_cache.lock().unwrap();

            if let Some(cache) = sample_cache.get_mut(acq_addr) {
                match cache.is_waiting_response {
                    true => {
                        if cache.msg_cache_queue.len()
                            >= self.concurrent_meter.meter_config.cache_queue_size
                        {
                            Err(ConcurrentMeterError::QueueLimit(
                                self.concurrent_meter.meter_config.cache_queue_size,
                            )
                            .into())
                        } else {
                            cache.msg_cache_queue.push_back(message);
                            cache.update_operation_time();
                            Ok(())
                        }
                    }
                    false => {
                        cache.is_waiting_response = true;
                        Self::mqtt_meter_reading_handler(
                            message,
                            master_address,
                            concurrent_msg_sender,
                        )
                    }
                }
            } else if Self::concurrent_addr_num(sample_cache.deref())
                >= self.concurrent_meter.meter_config.concurrent_addr
            {
                Err(ConcurrentMeterError::AddrLimit(
                    self.concurrent_meter.meter_config.concurrent_addr,
                )
                .into())
            } else {
                sample_cache.insert(acq_addr.to_string(), SampleCache::new());
                Self::mqtt_meter_reading_handler(message, master_address, concurrent_msg_sender)
            }
        };

        mqtt_msg_sender.send(MqttMessage::new(
            topic,
            MqttPayload::new_with_token_result(token, result),
        ))?;
        Ok(())
    }

    fn concurrent_acq_addr(message: &UartMessage) -> String {
        let mqtt_req_info = message.req_info.mqtt_req_info().unwrap();
        let extra_data = mqtt_req_info
            .extra_data()
            .unwrap()
            .downcast_ref::<SamplePayload>()
            .unwrap();
        extra_data.acq_addr.clone()
    }

    pub fn uart_meter_reading(
        &self,
        message: UartMessage,
        master_address: Address,
        mqtt_msg_sender: &mpsc::Sender<MqttMessage>,
        concurrent_msg_sender: &mpsc::Sender<UartMessage>,
    ) -> Result<()> {
        let acq_addr = Self::concurrent_acq_addr(&message);
        uart_response_mqtt_handler::<ConcurrentReadMeterResponse>(message, mqtt_msg_sender)?;
        self.handle_next_request(acq_addr, master_address, concurrent_msg_sender)
    }
}
