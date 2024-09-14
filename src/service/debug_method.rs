use serde::{Deserialize, Serialize};
use std::sync::mpsc;

use crate::mqtt_handler::MqttTopicType;
use crate::mqtt_message::PayloadBody;
use crate::protocol::app_data::Afn;
use crate::protocol::Frame;
use crate::request_info::{FrameKey, UartMessage};
use crate::{MqttMessage, MqttMsgHandler, ReqInfo, Result, APP_NAME};

pub struct DebugMethod;

#[derive(Debug, Serialize, Deserialize)]
struct DebugFrame {
    frame: String,
}

impl DebugMethod {
    pub fn mqtt_debug_frame_request(
        message: MqttMessage,
        uart_msg_sender: &mpsc::Sender<UartMessage>,
    ) -> Result<()> {
        let data: DebugFrame = serde_json::from_str(message.payload()).unwrap();
        let frame = Frame::try_from(hex::decode(data.frame)?.as_slice())?;
        let req_info = ReqInfo::new_with_key(
            &frame,
            FrameKey::new(Afn::Test, 0),
            Some(message.to_mqtt_req_info()),
        );
        uart_msg_sender
            .send(UartMessage::new(req_info, frame))
            .unwrap();

        Ok(())
    }

    pub fn uart_debug_frame_response(
        message: UartMessage,
        sender: &mpsc::Sender<MqttMessage>,
    ) -> Result<()> {
        let response = DebugFrame {
            frame: hex::encode(message.frame.to_bytes()),
        };
        let response = serde_json::to_value(response).unwrap();
        let mqtt_req_info = message.req_info.into_mqtt_req_info().unwrap();
        let mqtt_message =
            MqttMessage::new_with_req_info_body(mqtt_req_info, Some(PayloadBody::Flat(response)));

        sender.send(mqtt_message).unwrap();

        Ok(())
    }

    pub fn init(mqtt_msg_handler: &mut MqttMsgHandler) {
        use crate::config::SCHEMA_PATH;
        use crate::schema_check;
        let topic = format!("{}{}{}", "+/get/request/", APP_NAME, "/sendFrame");
        let schema =
            schema_check::parse_schema(SCHEMA_PATH.join("send_debug_frame_schema.json")).ok();
        mqtt_msg_handler.add_topic_filter(topic, MqttTopicType::SendDebugFrame, schema);
    }
}
