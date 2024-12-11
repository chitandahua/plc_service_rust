use serde::{Deserialize, Serialize};

use crate::mqtt_handler::MqttTopicType;
use crate::mqtt_message::PayloadBody;
use crate::protocol::app_data::{
    BroadcastDelayRequest, BroadcastDelayResponse, BroadcastRequest, ConfirmResponse,
};

use crate::request_info::UartMessage;
use crate::service::parse_response::{mqtt_request_handler, uart_response_handler};
use crate::{MqttMessage, MqttMsgHandler, Result};

use crate::service::{IntoMqttMessage, MqttReqInfo, RouteCtrl};
use crate::{impl_into_mqtt_message, register_mqtt_request_topics};
use std::sync::mpsc;

#[derive(Clone)]
pub struct Broadcast;

#[derive(Debug, Deserialize)]
struct MqttBroadCastDelayRequest {
    #[serde(rename = "proType")]
    pro_type: u8,
    data: String,
}

impl From<MqttBroadCastDelayRequest> for BroadcastDelayRequest {
    fn from(req: MqttBroadCastDelayRequest) -> Self {
        BroadcastDelayRequest::new(req.pro_type, hex::decode(req.data).unwrap())
    }
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

impl_into_mqtt_message!(MqttBroadCastDelayResponse, flat);

type MqttBroadCastCmdRequest = MqttBroadCastDelayRequest;

impl From<MqttBroadCastCmdRequest> for BroadcastRequest {
    fn from(req: MqttBroadCastCmdRequest) -> Self {
        BroadcastRequest::new(req.pro_type, hex::decode(req.data).unwrap())
    }
}

impl Broadcast {
    pub fn init(mqtt_msg_handler: &mut MqttMsgHandler) {
        register_mqtt_request_topics!(
            mqtt_msg_handler,
            (
                "get",
                "BroadcastDelay",
                MqttTopicType::BroadcastDelay,
                "broadcast_delay_schema.json"
            ),
            (
                "get",
                "BroadcastCmd",
                MqttTopicType::BroadcastCmd,
                "broadcast_cmd_schema.json"
            ),
        )
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
                .send(result.into_mqtt_message(message.to_mqtt_req_info()))
                .unwrap();
            return;
        }

        mqtt_request_handler::<BroadcastDelayRequest, MqttBroadCastCmdRequest>(
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
        uart_response_handler::<BroadcastDelayResponse, MqttBroadCastDelayResponse>(
            message,
            mqtt_msg_sender,
        )
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
                .send(result.into_mqtt_message(message.to_mqtt_req_info()))
                .unwrap();
            return;
        }

        mqtt_request_handler::<BroadcastRequest, MqttBroadCastCmdRequest>(message, uart_msg_sender);
    }

    pub fn uart_broadcast_cmd_response(
        route_ctrl: &RouteCtrl,
        message: UartMessage,
        mqtt_msg_sender: &mpsc::Sender<MqttMessage>,
        uart_msg_sender: &mpsc::Sender<UartMessage>,
    ) -> Result<()> {
        route_ctrl.uart_response_update_resume_timer(uart_msg_sender);
        uart_response_handler::<ConfirmResponse, ()>(message, mqtt_msg_sender)
    }
}
