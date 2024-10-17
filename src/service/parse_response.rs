use std::sync::mpsc;

use crate::mqtt_message::{MqttMessage, Status};
use crate::protocol::app_data::{ConfirmResponse, DenyResponse};
use crate::protocol::{AppData, Frame};
use crate::request_info::{MqttReqInfo, UartMessage};
use crate::service::IntoMqttMessage;
use crate::{ReqInfo, Result};

pub enum UartResponse<T> {
    Normal(T),
    Deny(DenyResponse),
}

impl<T> TryFrom<Frame> for UartResponse<T>
where
    T: TryFrom<AppData, Error = crate::Error>,
{
    type Error = crate::Error;
    fn try_from(frame: Frame) -> Result<Self> {
        Ok(match frame.is_deny() {
            true => UartResponse::Deny(DenyResponse::try_from(frame.into_app_data())?),
            false => UartResponse::Normal(T::try_from(frame.into_app_data())?),
        })
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

impl From<DenyResponse> for anyhow::Error {
    fn from(value: DenyResponse) -> Self {
        anyhow::anyhow!(value.error_code())
    }
}

impl<T> From<UartResponse<T>> for Result<T> {
    fn from(value: UartResponse<T>) -> Self {
        match value {
            UartResponse::Deny(response) => Err(response.into()),
            UartResponse::Normal(t) => Ok(t),
        }
    }
}

impl From<UartResponse<ConfirmResponse>> for Result<()> {
    fn from(value: UartResponse<ConfirmResponse>) -> Self {
        match value {
            UartResponse::Deny(response) => Err(response.into()),
            UartResponse::Normal(_) => Ok(()),
        }
    }
}

pub fn mqtt_response_message(result: Result<()>, mqtt_req_info: MqttReqInfo) -> MqttMessage {
    match result {
        Ok(_) => MqttMessage::new_with_req_info_body(mqtt_req_info, None),
        Err(e) => MqttMessage::new_with_req_info_status_reason(mqtt_req_info, Status::Failure, e),
    }
}

pub(crate) fn _uart_response_handler<
    T: IntoMqttMessage + TryFrom<AppData, Error = crate::Error>,
>(
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

pub(crate) fn mqtt_request_uart_handler<T: Into<AppData>>(
    app_data: T,
    message: MqttMessage,
    uart_msg_sender: &mpsc::Sender<UartMessage>,
) {
    let mqtt_req_info = MqttReqInfo::new(message.topic(), message.get_token(), None);
    mqtt_info_request_uart_handler::<T>(app_data, Some(mqtt_req_info), uart_msg_sender);
}

pub(crate) fn mqtt_info_request_uart_handler<T: Into<AppData>>(
    app_data: T,
    mqtt_req_info: Option<MqttReqInfo>,
    uart_msg_sender: &mpsc::Sender<UartMessage>,
) {
    let frame = Frame::new_request(None, app_data);
    let req_info = ReqInfo::new(&frame, mqtt_req_info);
    uart_msg_sender
        .send(UartMessage::new(req_info, frame))
        .unwrap();
}

pub(crate) fn uart_response_mqtt_handler<
    T: IntoMqttMessage + TryFrom<AppData, Error = crate::Error>,
>(
    message: UartMessage,
    mqtt_msg_sender: &mpsc::Sender<MqttMessage>,
) -> Result<()> {
    let response = UartResponse::<T>::try_from(message.frame)?;
    let mqtt_req_info = message.req_info.into_mqtt_req_info().unwrap();
    let response_msg = response.into_mqtt_message(mqtt_req_info);
    mqtt_msg_sender.send(response_msg)?;

    Ok(())
}
