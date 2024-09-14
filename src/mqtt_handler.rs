use crate::mqtt_message::Status;
use crate::service::{ChipInfo, MasterAddress, ModuleInfo, ModuleService};
use crate::{schema_check, MqttHandler, MqttResponseError, PlcDevice, Result};
use crate::{MqttMessage, UartMessage};

use jsonschema::JSONSchema;
use paho_mqtt::TopicFilter;
use std::sync::atomic::AtomicU64;
use std::sync::{mpsc, Condvar};
use tracing::{debug, error, warn};

use std::collections::BinaryHeap;
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};

#[derive(Debug)]
struct PriorityMessage {
    priority: u64,
    sequence: u64,
    message: MqttMessage,
}

impl Eq for PriorityMessage {}

impl PartialEq for PriorityMessage {
    fn eq(&self, other: &Self) -> bool {
        self.priority == other.priority && self.sequence == other.sequence
    }
}

impl Ord for PriorityMessage {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // priority越小优先级越高 => sequence越小优先级越高
        self.priority
            .cmp(&other.priority)
            .reverse()
            .cmp(&self.sequence.cmp(&other.sequence).reverse())
    }
}

impl PartialOrd for PriorityMessage {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

static SEQUENCE: AtomicU64 = AtomicU64::new(0);
impl PriorityMessage {
    pub fn new(message: MqttMessage) -> Self {
        Self {
            priority: message.get_priority(),
            sequence: AtomicU64::fetch_add(&SEQUENCE, 1, std::sync::atomic::Ordering::Relaxed),
            message,
        }
    }
}

struct PriorityQueue {
    queue: Mutex<BinaryHeap<PriorityMessage>>,
    cond: Condvar,
}

impl PriorityQueue {
    fn new() -> Self {
        PriorityQueue {
            queue: Mutex::new(BinaryHeap::new()),
            cond: Condvar::new(),
        }
    }

    fn push(&self, message: PriorityMessage) {
        let mut queue = self.queue.lock().unwrap();
        queue.push(message);
        self.cond.notify_one();
    }

    fn pop(&self) -> PriorityMessage {
        let mut queue = self.queue.lock().unwrap();
        while queue.is_empty() {
            queue = self.cond.wait(queue).unwrap();
        }
        queue.pop().unwrap()
    }
}

#[derive(Debug, PartialEq)]
pub enum MqttTopicType {
    GetModuleInfo,
    GetMasterIdInfo,
    // 主地址
    GetMasterAddress,
    SetMasterAddress,
    // 档案
    AddAcqFiles,
    GetAcqFiles,
    GetAcqFilesNum,
    DelAcqFiles,
    ClearAcqFiles,
    // 并发抄表
    ConcurrentMeter,
    // HPLC信息
    GetChipInfo,
}

struct MqttTopicFilter {
    topic: String,
    mqtt_topic_type: MqttTopicType,
    filter: TopicFilter,
    schema: Option<JSONSchema>,
}

pub struct MqttMsgHandler {
    mqtt_msg_sender: mpsc::Sender<MqttMessage>,
    uart_msg_sender: mpsc::Sender<UartMessage>,
    concurrent_msg_sender: mpsc::Sender<UartMessage>,
    topic_filters: Vec<MqttTopicFilter>,
    priority_queue: Arc<PriorityQueue>,
    msg_receiver: mpsc::Receiver<MqttMessage>,
}

impl MqttMsgHandler {
    pub fn new(
        mqtt_msg_sender: mpsc::Sender<MqttMessage>,
        uart_msg_sender: mpsc::Sender<UartMessage>,
        concurrent_msg_sender: mpsc::Sender<UartMessage>,
        msg_receiver: mpsc::Receiver<MqttMessage>,
    ) -> Self {
        Self {
            mqtt_msg_sender,
            uart_msg_sender,
            concurrent_msg_sender,
            topic_filters: Vec::new(),
            priority_queue: Arc::new(PriorityQueue::new()),
            msg_receiver,
        }
    }

    pub fn run(self, services: ModuleService) -> Result<Vec<JoinHandle<()>>> {
        debug!("mqtt msg handler run");
        let MqttMsgHandler {
            mqtt_msg_sender,
            uart_msg_sender,
            concurrent_msg_sender,
            topic_filters,
            priority_queue,
            msg_receiver,
        } = self;
        let priority_queue_clone = priority_queue.clone();
        let mqtt_msg_sender_clone = mqtt_msg_sender.clone();
        let topic_filters = Arc::new(topic_filters);
        let topic_filters_clone = topic_filters.clone();

        let msg_handle = thread::spawn(move || loop {
            let message = msg_receiver.recv().unwrap();

            // schema check
            let schema = topic_filters_clone
                .iter()
                .find(|&topic_filter| topic_filter.filter.matches(message.topic()))
                .and_then(|topic_filter| topic_filter.schema.as_ref());

            if let Some(schema) = schema {
                match schema_check::schema_check(schema, message.payload()) {
                    Ok(_) => {}
                    Err(e) => {
                        error!("schema check failed: {}", e);
                        mqtt_msg_sender_clone
                            .send(MqttMessage::new_with_msg_status_reason(
                                message,
                                Status::Failure,
                                MqttResponseError::InvalidJson(e.to_string()),
                            ))
                            .unwrap();
                        continue;
                    }
                }
            }

            let priority_message = PriorityMessage::new(message);
            priority_queue_clone.push(priority_message);
        });

        let handle = thread::spawn(move || loop {
            let priority_message = priority_queue.pop();
            debug!("priority message: {:?}", priority_message);
            let message = priority_message.message;

            let topic = message.topic();
            let sub_topic = topic_filters
                .iter()
                .find(|&topic_filter| topic_filter.filter.matches(topic));

            if let Some(sub_topic) = sub_topic {
                let result = match sub_topic.mqtt_topic_type {
                    MqttTopicType::GetModuleInfo => {
                        ModuleInfo::mqtt_get_module_info(message, &uart_msg_sender);
                        Ok(())
                    }
                    MqttTopicType::GetMasterIdInfo => {
                        ModuleInfo::mqtt_get_master_id_info(message, &uart_msg_sender);
                        Ok(())
                    }
                    MqttTopicType::GetMasterAddress => services
                        .master_address
                        .mqtt_get_address(message, &mqtt_msg_sender),
                    MqttTopicType::SetMasterAddress => {
                        MasterAddress::mqtt_set_address(message, &uart_msg_sender)
                    }
                    MqttTopicType::AddAcqFiles => services.node_manage.mqtt_add_acq_files(
                        message,
                        &mqtt_msg_sender,
                        &uart_msg_sender,
                    ),
                    MqttTopicType::DelAcqFiles => services.node_manage.mqtt_del_acq_files(
                        message,
                        &mqtt_msg_sender,
                        &uart_msg_sender,
                    ),
                    MqttTopicType::ClearAcqFiles => services.node_manage.mqtt_clear_acq_files(
                        message,
                        &mqtt_msg_sender,
                        &uart_msg_sender,
                    ),
                    MqttTopicType::GetAcqFiles => services.node_manage.mqtt_get_acq_files(
                        message,
                        &mqtt_msg_sender,
                        &uart_msg_sender,
                    ),
                    MqttTopicType::GetAcqFilesNum => services
                        .node_manage
                        .mqtt_get_acq_files_number(message, &mqtt_msg_sender, &uart_msg_sender),
                    MqttTopicType::ConcurrentMeter => {
                        let master_address = services.master_address.get_master_address();
                        services.concurrent_meter.mqtt_concurrent_meter_reading(
                            message,
                            master_address,
                            &mqtt_msg_sender,
                            &concurrent_msg_sender,
                        )
                    }
                    MqttTopicType::GetChipInfo => {
                        ChipInfo::mqtt_get_chip_info(message, &uart_msg_sender);
                        Ok(())
                    }
                };

                if let Err(e) = result {
                    error!("mqtt msg handler error: {}", e);
                }
            } else {
                warn!("unrecognized topic: {}", topic);
            }
        });

        Ok(vec![handle, msg_handle])
    }

    pub fn subscribe_topics(&self) -> Vec<String> {
        self.topic_filters
            .iter()
            .map(|filter| filter.topic.clone())
            .collect()
    }

    pub fn add_topic_filter(
        &mut self,
        topic: String,
        mqtt_topic_type: MqttTopicType,
        schema: Option<JSONSchema>,
    ) {
        let filter = TopicFilter::new(&topic).unwrap();
        self.topic_filters.push(MqttTopicFilter {
            topic,
            mqtt_topic_type,
            filter,
            schema,
        });
    }
}

pub struct Handler {
    msg_sender: mpsc::Sender<MqttMessage>,
    topics: Vec<String>,
    plc_device: PlcDevice,
}

impl Handler {
    pub fn new(
        msg_sender: mpsc::Sender<MqttMessage>,
        topics: Vec<String>,
        plc_device: PlcDevice,
    ) -> Self {
        Self {
            msg_sender,
            topics,
            plc_device,
        }
    }
}

impl MqttHandler for Handler {
    fn mqtt_msg_handler(&mut self, message: MqttMessage) -> Result<Option<MqttMessage>> {
        debug!(
            "mqtt msg handler: topic {}, payload {}",
            message.topic(),
            message.payload()
        );

        if !self.plc_device.available() {
            return Ok(Some(MqttMessage::new_with_msg_status_reason(
                message,
                Status::Failure,
                MqttResponseError::ModelOffline,
            )));
        }

        self.msg_sender.send(message)?;
        Ok(None)
    }

    fn subscribe_topics(&self) -> Vec<String> {
        self.topics.to_owned()
    }
}
