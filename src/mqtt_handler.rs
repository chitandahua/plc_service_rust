use crate::protocol::app_data::ModuleInfoRequest;
use crate::protocol::Frame;
use crate::request_info::ReqInfo;
use crate::service::{MasterAddress, ModuleInfo, ModuleService};
use crate::{MqttClient, MqttHandler, Result};
use crate::{MqttMessage, TopicError, UartMessage};

use paho_mqtt::TopicFilter;
use std::sync::atomic::AtomicU64;
use std::sync::{mpsc, Condvar};
use tracing::{debug, error, warn};

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
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
    // 主地址
    GetMasterAddress,
    SetMasterAddress,
    // 档案
    AddAcqFiles,
    GetAcqFiles,
    GetAcqFilesNum,
    DelAcqFiles,
    ClearAcqFiles,
}

struct MqttTopicFilter {
    topic: String,
    mqtt_topic_type: MqttTopicType,
    filter: TopicFilter,
}

pub struct MqttMsgHandler {
    mqtt_msg_sender: mpsc::Sender<MqttMessage>,
    uart_msg_sender: mpsc::Sender<UartMessage>,
    topic_filters: Vec<MqttTopicFilter>,
    priority_queue: Arc<PriorityQueue>,
    msg_receiver: mpsc::Receiver<MqttMessage>,
}

impl MqttMsgHandler {
    pub fn new(
        mqtt_msg_sender: mpsc::Sender<MqttMessage>,
        uart_msg_sender: mpsc::Sender<UartMessage>,
        msg_receiver: mpsc::Receiver<MqttMessage>,
    ) -> Self {
        Self {
            mqtt_msg_sender,
            uart_msg_sender,
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
            topic_filters,
            priority_queue,
            msg_receiver,
        } = self;
        let priority_queue_clone = priority_queue.clone();

        let msg_handle = thread::spawn(move || loop {
            let message = msg_receiver.recv().unwrap();
            let priority = message.get_priority();
            let priority_message = PriorityMessage::new(message);
            priority_queue_clone.push(priority_message);
        });

        let handle = thread::spawn(move || {
            loop {
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
                            ModuleInfo::mqtt_get_module_info(message, &uart_msg_sender)
                        }
                        MqttTopicType::GetMasterAddress => services
                            .master_address
                            .mqtt_get_address(message, &mqtt_msg_sender, &uart_msg_sender),
                        MqttTopicType::SetMasterAddress => {
                            MasterAddress::mqtt_set_address(message, &uart_msg_sender)
                        }
                        MqttTopicType::AddAcqFiles => services.node_manage.mqtt_add_acq_files(
                            message,
                            &mqtt_msg_sender,
                            &uart_msg_sender,
                        ),
                        _ => {
                            error!("unrecognized topic: {}", topic);
                            Ok(())
                        }
                    };

                    if let Err(e) = result {
                        error!("mqtt msg handler error: {}", e);
                    }
                } else {
                    warn!("unrecognized topic: {}", topic);
                }
            }

            warn!("mqtt msg handler exit");
        });

        Ok(vec![handle, msg_handle])
    }

    pub fn subscribe_topics(&self) -> Vec<String> {
        self.topic_filters
            .iter()
            .map(|filter| filter.topic.clone())
            .collect()
    }

    pub fn add_topic_filter(&mut self, topic: String, mqtt_topic_type: MqttTopicType) {
        let filter = TopicFilter::new(&topic).unwrap();
        self.topic_filters.push(MqttTopicFilter {
            topic,
            mqtt_topic_type,
            filter,
        });
    }

    pub fn add_topic_filters(&mut self, topic_filters: Vec<(String, MqttTopicType)>) {
        topic_filters
            .into_iter()
            .for_each(|(topic, mqtt_topic_type)| self.add_topic_filter(topic, mqtt_topic_type));
    }
}

pub struct Handler {
    msg_sender: mpsc::Sender<MqttMessage>,
    topics: Vec<String>,
}

impl Handler {
    pub fn new(msg_sender: mpsc::Sender<MqttMessage>, topics: Vec<String>) -> Self {
        Self { msg_sender, topics }
    }
}

impl MqttHandler for Handler {
    fn mqtt_msg_handler(&mut self, message: MqttMessage) -> Result<Option<MqttMessage>> {
        debug!(
            "mqtt msg handler: topic {}, payload {}",
            message.topic(),
            message.payload()
        );

        self.msg_sender.send(message)?;
        Ok(None)
    }

    fn subscribe_topics(&self) -> Vec<String> {
        self.topics.to_owned()
    }
}
