use serde_json::Value;
use std::sync::mpsc;

use crate::mqtt_message::{MqttMessage, MqttPayload};
use crate::protocol::app_data::{
    self, ConfirmResponse, DenyResponse, QueryNodeInfoResponse, QueryNodeNumberRequest,
    QueryNodeNumberResponse,
};
use crate::protocol::AppData;
use crate::protocol::Frame;
use crate::request_info::{MqttReqInfo, ReqInfo, UartMessage};
use crate::Result;

use crate::service::module_info;
use crate::service::node_config::NodeInfo;
use crate::service::node_manage::NodeNumerResponse;

pub enum UartResponse<T> {
    Normal(T),
    Deny(DenyResponse),
}

impl<T> TryFrom<Frame> for UartResponse<T>
where
    T: TryFrom<AppData>,
{
    type Error = crate::Error;
    fn try_from(frame: Frame) -> Result<Self> {
        if frame.is_deny() {
            Ok(UartResponse::Deny(DenyResponse::try_from(
                frame.into_app_data(),
            )?))
        } else if let Ok(response) = T::try_from(frame.into_app_data()) {
            Ok(UartResponse::Normal(response))
        } else {
            anyhow::bail!("invalid response frame")
        }
    }
}

impl<T> IntoMqttMessage for UartResponse<T>
where
    T: IntoMqttMessage,
{
    fn into_mqtt_message(self, mqtt_req_info: MqttReqInfo) -> MqttMessage {
        match self {
            UartResponse::Normal(response) => response.into_mqtt_message(mqtt_req_info),
            UartResponse::Deny(response) => response.into_mqtt_message(mqtt_req_info),
        }
    }
}

impl From<UartResponse<ConfirmResponse>> for Result<()> {
    fn from(value: UartResponse<ConfirmResponse>) -> Self {
        match value {
            UartResponse::Deny(response) => Err(anyhow::anyhow!(response.error_code())),
            UartResponse::Normal(_) => Ok(()),
        }
    }
}

pub trait IntoMqttMessage {
    fn into_mqtt_message(self, mqtt_req_info: MqttReqInfo) -> MqttMessage;
}

impl IntoMqttMessage for ConfirmResponse {
    fn into_mqtt_message(self, mqtt_req_info: MqttReqInfo) -> MqttMessage {
        let payload = MqttPayload::new_with_token_status_reason(mqtt_req_info.token(), "OK", "OK");
        MqttMessage::new(mqtt_req_info.topic(), payload)
    }
}

impl IntoMqttMessage for DenyResponse {
    fn into_mqtt_message(self, mqtt_req_info: MqttReqInfo) -> MqttMessage {
        let payload = MqttPayload::new_with_token_status_reason(
            mqtt_req_info.token(),
            "FAILURE",
            self.error_code(),
        );
        MqttMessage::new(mqtt_req_info.topic(), payload)
    }
}

impl IntoMqttMessage for QueryNodeNumberResponse {
    fn into_mqtt_message(self, mqtt_req_info: MqttReqInfo) -> MqttMessage {
        MqttMessage::new_with_req_info_body(
            mqtt_req_info,
            Some(serde_json::to_value(NodeNumerResponse::from(self)).unwrap()),
        )
    }
}

impl IntoMqttMessage for QueryNodeInfoResponse {
    fn into_mqtt_message(self, mqtt_req_info: MqttReqInfo) -> MqttMessage {
        let node_infos: Vec<NodeInfo> = self
            .into_node_infos()
            .into_iter()
            .map(|n| n.into())
            .collect();
        MqttMessage::new_with_req_info_body(
            mqtt_req_info,
            Some(serde_json::to_value(&node_infos).unwrap()),
        )
    }
}

impl IntoMqttMessage for app_data::ModuleInfoResponse {
    fn into_mqtt_message(self, mqtt_req_info: MqttReqInfo) -> MqttMessage {
        let response = module_info::ModuleInfoResponse::from(self);

        MqttMessage::new_with_req_info_body(
            mqtt_req_info,
            Some(serde_json::to_value(response).unwrap()),
        )
    }
}

pub(crate) fn uart_response_handler<T: IntoMqttMessage + TryFrom<AppData>>(
    init_handler: impl Fn(u8, UartResponse<T>, &mpsc::Sender<UartMessage>) -> Result<()>,
    message: UartMessage,
    mqtt_msg_sender: &mpsc::Sender<MqttMessage>,
    uart_msg_sender: &mpsc::Sender<UartMessage>,
) -> Result<()> {
    let seq = message.frame.get_seq();
    let response = UartResponse::<T>::try_from(message.frame)?;
    let mqtt_req_info = message.req_info.into_mqtt_req_info();
    match mqtt_req_info {
        Some(mqtt_req_info) => {
            let response_msg = response.into_mqtt_message(mqtt_req_info);
            mqtt_msg_sender.send(response_msg)?;
        }
        None => {
            init_handler(seq, response, uart_msg_sender)?;
        }
    }

    Ok(())
}

pub(crate) fn uart_response_mqtt_handler<T: IntoMqttMessage + TryFrom<AppData>>(
    message: UartMessage,
    mqtt_msg_sender: &mpsc::Sender<MqttMessage>,
) -> Result<()> {
    let response = UartResponse::<T>::try_from(message.frame)?;
    let mqtt_req_info = message.req_info.into_mqtt_req_info().unwrap();
    let response_msg = response.into_mqtt_message(mqtt_req_info);
    mqtt_msg_sender.send(response_msg)?;

    Ok(())
}
