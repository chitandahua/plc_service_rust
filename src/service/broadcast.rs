use serde::{Deserialize, Serialize};

use crate::mqtt_handler::MqttTopicType;
use crate::mqtt_message::PayloadBody;
use crate::protocol::app_data::{
    BroadcastDelayRequest, BroadcastDelayResponse, BroadcastRequest, ConfirmResponse,
};

use crate::request_info::UartMessage;
use crate::service::parse_response::{
    mqtt_request_uart_handler, mqtt_response_message, uart_response_mqtt_handler,
};
use crate::{MqttMessage, MqttMsgHandler, Result, APP_NAME};

use crate::service::{IntoMqttMessage, MqttReqInfo, RouteCtrl};
use std::sync::mpsc;

#[derive(Clone)]
pub struct Broadcast;

#[derive(Debug, Deserialize)]
struct MqttBroadCastDelayRequest {
    #[serde(rename = "proType")]
    pro_type: u8,
    data: String,
}

#[derive(Debug, Serialize)]
struct MqttBroadCastDelayResponse {
    delay: u16,
}

impl From<BroadcastDelayResponse> for MqttBroadCastDelayResponse {
    fn from(response: BroadcastDelayResponse) -> Self {
        MqttBroadCastDelayResponse {
            delay: response.delay,
        }
    }
}

impl IntoMqttMessage for BroadcastDelayResponse {
    fn into_mqtt_message(self, mqtt_req_info: MqttReqInfo) -> MqttMessage {
        MqttMessage::new_with_req_info_body(
            mqtt_req_info,
            Some(PayloadBody::Flat(
                serde_json::to_value(MqttBroadCastDelayResponse::from(self)).unwrap(),
            )),
        )
    }
}

type MqttBroadCastCmdRequest = MqttBroadCastDelayRequest;

impl Broadcast {
    pub fn init(mqtt_msg_handler: &mut MqttMsgHandler) {
        use crate::config::SCHEMA_PATH;
        use crate::schema_check;
        let topic = format!("{}{}{}", "+/get/request/", APP_NAME, "/BroadcastDelay");
        let schema =
            schema_check::parse_schema(SCHEMA_PATH.join("broadcast_delay_schema.json")).ok();
        mqtt_msg_handler.add_topic_filter(topic, MqttTopicType::BroadcastDelay, schema);

        let topic = format!("{}{}{}", "+/get/request/", APP_NAME, "/BroadcastCmd");
        let schema = schema_check::parse_schema(SCHEMA_PATH.join("broadcast_cmd_schema.json")).ok();
        mqtt_msg_handler.add_topic_filter(topic, MqttTopicType::BroadcastCmd, schema);
    }

    pub fn mqtt_get_broadcast_delay(
        route_ctrl: &RouteCtrl,
        message: MqttMessage,
        mqtt_msg_sender: &mpsc::Sender<MqttMessage>,
        uart_msg_sender: &mpsc::Sender<UartMessage>,
    ) {
        let result = route_ctrl.auto_pause_metering(uart_msg_sender);
        if result.is_err() {
            mqtt_msg_sender
                .send(mqtt_response_message(result, message.to_mqtt_req_info()))
                .unwrap();
            return;
        }

        let request: MqttBroadCastDelayRequest = serde_json::from_str(message.payload()).unwrap();
        mqtt_request_uart_handler::<BroadcastDelayRequest>(
            BroadcastDelayRequest::new(request.pro_type, hex::decode(request.data).unwrap()),
            message,
            uart_msg_sender,
        );
    }

    pub fn uart_broadcast_delay_response(
        route_ctrl: &RouteCtrl,
        message: UartMessage,
        mqtt_msg_sender: &mpsc::Sender<MqttMessage>,
        uart_msg_sender: &mpsc::Sender<UartMessage>,
    ) -> Result<()> {
        route_ctrl.uart_response_update_resume_timer(uart_msg_sender);
        uart_response_mqtt_handler::<BroadcastDelayResponse>(message, mqtt_msg_sender)
    }

    pub fn mqtt_broadcast_cmd(
        route_ctrl: &RouteCtrl,
        message: MqttMessage,
        mqtt_msg_sender: &mpsc::Sender<MqttMessage>,
        uart_msg_sender: &mpsc::Sender<UartMessage>,
    ) {
        let result = route_ctrl.auto_pause_metering(uart_msg_sender);
        if result.is_err() {
            mqtt_msg_sender
                .send(mqtt_response_message(result, message.to_mqtt_req_info()))
                .unwrap();
            return;
        }

        let request: MqttBroadCastCmdRequest = serde_json::from_str(message.payload()).unwrap();
        mqtt_request_uart_handler::<BroadcastRequest>(
            BroadcastRequest::new(request.pro_type, hex::decode(request.data).unwrap()),
            message,
            uart_msg_sender,
        );
    }

    pub fn uart_broadcast_cmd_response(
        route_ctrl: &RouteCtrl,
        message: UartMessage,
        mqtt_msg_sender: &mpsc::Sender<MqttMessage>,
        uart_msg_sender: &mpsc::Sender<UartMessage>,
    ) -> Result<()> {
        route_ctrl.uart_response_update_resume_timer(uart_msg_sender);
        uart_response_mqtt_handler::<ConfirmResponse>(message, mqtt_msg_sender)
    }
}
