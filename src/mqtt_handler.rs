use crate::protocol::app_data::ModuleInfoRequest;
use crate::protocol::Frame;
use crate::request_info::ReqInfo;
use crate::{MqttClient, MqttHandler, Result};
use crate::{MqttMessage, TopicError, UartMessage};

use paho_mqtt::TopicFilter;
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

pub type CallBack<'a> = Arc<dyn Fn(MqttMessage) -> Result<()> + Send + Sync + 'a>; // fn(MqttMessage) -> Result<()>

pub struct MqttMsgHandler<'a> {
    pub mqtt_msg_sender: mpsc::Sender<MqttMessage>, // TODO 不需要？
    topics: Vec<String>,
    priority_queue: Arc<PriorityQueue>,
    req_callback: Vec<(TopicFilter, CallBack<'a>)>,
    res_callback: Vec<(TopicFilter, CallBack<'a>)>,
    msg_receiver: mpsc::Receiver<MqttMessage>,
}

impl<'a> MqttMsgHandler<'a> {
    pub fn new(
        mqtt_msg_sender: mpsc::Sender<MqttMessage>,
        msg_receiver: mpsc::Receiver<MqttMessage>,
    ) -> Self {
        Self {
            mqtt_msg_sender,
            topics: Vec::new(),
            priority_queue: Arc::new(PriorityQueue::new()),
            req_callback: Vec::new(),
            res_callback: Vec::new(),
            msg_receiver,
        }
    }

    pub fn run(self) -> Result<Vec<JoinHandle<()>>> {
        debug!("mqtt msg handler run");
        let MqttMsgHandler {
            mqtt_msg_sender,
            topics,
            priority_queue,
            req_callback,
            res_callback,
            msg_receiver,
        } = self;
        let priority_queue_clone = priority_queue.clone();
        let msg_handle = thread::spawn(move || loop {
            let message = msg_receiver.recv().unwrap();
            let priority = message.get_priority();
            let priority_message = PriorityMessage::new(message);
            priority_queue_clone.push(priority_message);
        });

        // 必须放最后... 否则会阻塞
        let handle = thread::scope(move |s| {
            s.spawn(move || {
                loop {
                    let priority_message = priority_queue.pop();
                    debug!("priority message: {:?}", priority_message);
                    let message = priority_message.message;

                    let topic = message.topic();
                    if let Some(handler) = req_callback.iter().find(|x| x.0.matches(topic)) {
                        if let Err(e) = handler.1(message) {
                            warn!("req callback error: {}", e);
                        }
                    } else if let Some(handler) = res_callback.iter().find(|x| x.0.matches(topic)) {
                        if let Err(e) = handler.1(message) {
                            warn!("res callback error: {}", e);
                        }
                    } else {
                        warn!("topic not found: {}", topic);
                        continue;
                    }
                }

                warn!("mqtt msg handler exit");
            })
            .join()
        });

        Ok(vec![handle.unwrap(), msg_handle])
    }

    pub fn subscribe_topics(&self) -> Vec<String> {
        self.topics.to_vec()
    }

    pub fn register_req_callback(&mut self, topic: String, callback: CallBack<'a>) {
        let topic_filter = TopicFilter::new(topic.as_str()).unwrap();
        self.topics.push(topic);
        self.req_callback.push((topic_filter, callback));
    }

    pub fn register_res_callback<'b>(&'b mut self, topic: String, callback: CallBack<'a>)
    where
        'a: 'b,
    {
        let topic_filter = TopicFilter::new(topic.as_str()).unwrap();
        self.topics.push(topic);
        self.res_callback.push((topic_filter, callback));
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
