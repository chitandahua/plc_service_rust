use crate::protocol::app_data::ModuleInfoRequest;
use crate::protocol::Frame;
use crate::request_info::ReqInfo;
use crate::{MqttClient, MqttHandler, Result};
use crate::{MqttMessage, TopicError, UartMessage};

use std::sync::atomic::AtomicU64;
use std::sync::{mpsc, Condvar};
use tracing::{debug, warn};

use std::cmp::Reverse;
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

#[derive(Clone)]
pub struct MqttMsgHandler {
    mqtt_msg_sender: mpsc::Sender<MqttMessage>, // TODO 不需要？
    uart_msg_sender: mpsc::Sender<UartMessage>,
    topics: Vec<String>,
    priority_queue: Arc<Mutex<BinaryHeap<PriorityMessage>>>,
    cond: Arc<Condvar>,
}

impl MqttMsgHandler {
    pub fn new(
        mqtt_msg_sender: mpsc::Sender<MqttMessage>,
        uart_msg_sender: mpsc::Sender<UartMessage>,
        topics: Vec<String>,
    ) -> Self {
        Self {
            mqtt_msg_sender,
            uart_msg_sender,
            topics,
            priority_queue: Arc::new(Mutex::new(BinaryHeap::new())),
            cond: Arc::new(Condvar::new()),
        }
    }

    pub fn run(&self) -> Result<JoinHandle<()>> {
        let MqttMsgHandler {
            mqtt_msg_sender,
            uart_msg_sender,
            topics,
            priority_queue,
            cond,
        } = self.clone();
        let handle = thread::spawn(move || {
            loop {
                let priority_message = {
                    let mut priority_queue = priority_queue.lock().unwrap();
                    while priority_queue.is_empty() {
                        priority_queue = cond.wait(priority_queue).unwrap();
                    }
                    priority_queue.pop().unwrap()
                };
                debug!("priority message: {:?}", priority_message);
                let message = priority_message.message;
                let frame = match message.topic() {
                    "app4/get/request/PLCServiceGW/modeInfo" => {
                        Some(Frame::new_request(ModuleInfoRequest.into()))
                    }
                    _ => {
                        warn!("unknown topic: {}", message.topic());
                        None
                    }
                };

                if let Some(frame) = frame {
                    let req_info =
                        ReqInfo::new_with_mqtt(&frame, message.topic(), message.get_token());
                    uart_msg_sender
                        .send(UartMessage::new(req_info, frame))
                        .unwrap();
                }
            }

            warn!("mqtt msg handler exit");
        });

        Ok(handle)
    }

    pub fn send(&self, message: MqttMessage) -> Result<()> {
        let priority = message.get_priority();
        let priority_message = PriorityMessage::new(message);
        let mut priority_queue = self.priority_queue.lock().unwrap();
        priority_queue.push(priority_message);
        self.cond.notify_one();
        Ok(())
    }

    pub fn subscribe_topics(&self) -> Vec<String> {
        self.topics.clone()
    }
}

pub struct Handler {
    mqtt_msg_handler: Arc<MqttMsgHandler>,
}

impl Handler {
    pub fn new(mqtt_msg_handler: Arc<MqttMsgHandler>) -> Self {
        Self { mqtt_msg_handler }
    }
}

impl MqttHandler for Handler {
    fn mqtt_msg_handler(&mut self, message: MqttMessage) -> Result<Option<MqttMessage>> {
        debug!(
            "mqtt msg handler: topic {}, payload {}",
            message.topic(),
            message.payload()
        );

        self.mqtt_msg_handler.send(message)?;
        Ok(None)
    }

    fn subscribe_topics(&self) -> Vec<String> {
        self.mqtt_msg_handler.subscribe_topics()
    }
}
