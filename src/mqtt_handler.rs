use crate::{MqttClient, MqttHandler, Result};
use crate::{MqttMessage, TopicError};

use std::sync::atomic::AtomicU64;
use std::sync::mpsc;
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
    topics: Vec<String>,
    priority_queue: Arc<Mutex<BinaryHeap<PriorityMessage>>>,
}

impl MqttMsgHandler {
    pub fn new(mqtt_msg_sender: mpsc::Sender<MqttMessage>, topics: Vec<String>) -> Self {
        Self {
            mqtt_msg_sender,
            topics,
            priority_queue: Arc::new(Mutex::new(BinaryHeap::new())),
        }
    }

    pub fn run(&self) -> Result<JoinHandle<()>> {
        let MqttMsgHandler {
            mqtt_msg_sender,
            topics,
            priority_queue,
        } = self.clone();
        let handle = thread::spawn(move || {
            loop {
                let mut priority_queue = priority_queue.lock().unwrap();
                if let Some(priority_message) = priority_queue.peek() {
                    //let message = priority_message.message.clone();
                    //drop(priority_queue);
                    //if let Err(e) = mqtt_msg_sender.send(message) {
                    //    warn!("mqtt msg send error: {:?}", e);
                    //}
                } else {
                    break;
                }
            }
        });

        Ok(handle)
    }

    pub fn send(&self, message: MqttMessage) -> Result<()> {
        let priority = message.get_priority();
        let priority_message = PriorityMessage::new(message);
        let mut priority_queue = self.priority_queue.lock().unwrap();
        priority_queue.push(priority_message);
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
