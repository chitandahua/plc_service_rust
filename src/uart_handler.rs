use std::sync::mpsc;
use tracing::debug;

use crate::protocol::app_data::Afn;
use crate::request_info::FrameKey;
use crate::MqttMessage;
use crate::{Result, UartHandler, UartMessage};

use crate::ModuleInfo;

pub struct UartMsgHandler {
    pub mqtt_msg_sender: mpsc::Sender<MqttMessage>,
    pub uart_msg_sender: mpsc::Sender<UartMessage>,
}

impl UartMsgHandler {
    pub fn new(
        mqtt_msg_sender: mpsc::Sender<MqttMessage>,
        uart_msg_sender: mpsc::Sender<UartMessage>,
    ) -> Self {
        Self {
            mqtt_msg_sender,
            uart_msg_sender,
        }
    }
}

impl UartHandler for UartMsgHandler {
    fn uart_msg_handler(&mut self, message: UartMessage) -> Result<()> {
        debug!(
            "uart msg handler: AFN: {:02x}, Fn: {}",
            message.req_info.frame_key().afn(),
            message.req_info.frame_key().fn_num()
        );

        match message.req_info.frame_key().to_tuple() {
            (Afn::QueryData, 10) => {
                ModuleInfo::module_info_response(message, self.mqtt_msg_sender.clone())?;
            }
            _ => {}
        }

        Ok(())
    }
}
