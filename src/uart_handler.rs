use std::sync::mpsc;
use tracing::debug;

use crate::request_info::FrameKey;
use crate::MqttMessage;
use crate::{Result, UartHandler, UartMessage};

use crate::ModuleInfo;

pub struct UartMsgHandler {
    mqtt_msg_sender: mpsc::Sender<MqttMessage>,
}

impl UartMsgHandler {
    pub fn new(mqtt_msg_sender: mpsc::Sender<MqttMessage>) -> Self {
        Self { mqtt_msg_sender }
    }
}

impl UartHandler for UartMsgHandler {
    fn uart_msg_handler(&mut self, message: UartMessage) -> Result<()> {
        debug!(
            "uart msg handler: AFN: {:02x}, Fn: {}",
            message.req_info.frame_key.0, message.req_info.frame_key.1
        );

        match message.req_info.frame_key {
            FrameKey(0x03, 10) => {
                ModuleInfo::module_info_response(message, &self.mqtt_msg_sender)?;
            }
            _ => {}
        }

        Ok(())
    }
}
