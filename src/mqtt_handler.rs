use crate::{MqttClient, MqttHandler, Result};
use crate::{MqttMessage, TopicError};

use std::sync::mpsc;
use tracing::{debug, warn};

pub struct Handler {
    mqtt_msg_sender: mpsc::Sender<MqttMessage>,
    topics: Vec<String>,
}

impl Handler {
    pub fn new(client: &MqttClient) -> Self {
        Self {
            mqtt_msg_sender: client.sender().clone(),
            topics: vec!["test".to_string()],
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

        Ok(None)
    }

    fn subscribe_topics(&self) -> &[String] {
        &self.topics
    }
}
