use std::sync::mpsc;
use std::sync::LazyLock;

use serde::Serialize;

use crate::mqtt_message::{MqttMessage, MqttPayload, PayloadBody};
use crate::protocol::app_data::{ConfirmResponse, SlaveNodeEvent};
use crate::protocol::Frame;
use crate::request_info::{ReqInfo, UartMessage};
use crate::{Result, APP_NAME};

pub struct EventReport;

#[derive(Debug, Serialize)]
struct MqttSlaveNodeEvent {
    #[serde(rename = "deviceType")]
    device_type: u8,
    #[serde(rename = "protocolType")]
    protocol_type: u8,
    #[serde(rename = "data")]
    data: String,
}

impl From<SlaveNodeEvent> for MqttSlaveNodeEvent {
    fn from(event: SlaveNodeEvent) -> Self {
        Self {
            device_type: event.device_type,
            protocol_type: event.protocol_type,
            data: hex::encode(event.data),
        }
    }
}

impl EventReport {
    pub fn uart_slave_node_event_report(
        message: UartMessage,
        uart_msg_sender: &mpsc::Sender<UartMessage>,
        mqtt_msg_sender: &mpsc::Sender<MqttMessage>,
    ) -> Result<()> {
        static EVENT_TOPIC: LazyLock<String> =
            LazyLock::new(|| format!("{}/notify/spont/*/event", APP_NAME));
        let frame = Frame::new_response(message.frame.get_seq(), None, ConfirmResponse::default());
        let response = UartMessage::new(ReqInfo::new(&message.frame, None), frame);
        uart_msg_sender.send(response).unwrap();

        let payload = MqttPayload::new_with_body(Some(PayloadBody::Nested {
            body: serde_json::to_value(MqttSlaveNodeEvent::from(SlaveNodeEvent::try_from(
                message.frame.into_app_data(),
            )?))
            .unwrap(),
        }));
        let message = MqttMessage::new((*EVENT_TOPIC).clone(), payload);
        mqtt_msg_sender.send(message).unwrap();
        Ok(())
    }
}
