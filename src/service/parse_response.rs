use std::sync::mpsc;

use crate::mqtt_message::MqttMessage;
use crate::protocol::app_data::DenyResponse;
use crate::protocol::{AppData, Frame};
use crate::request_info::{MqttReqInfo, UartMessage};
use crate::service::IntoMqttMessage;
use crate::Result;

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

impl From<DenyResponse> for anyhow::Error {
    fn from(value: DenyResponse) -> Self {
        anyhow::anyhow!(value.error_code())
    }
}

impl<T> From<UartResponse<T>> for Result<()> {
    fn from(value: UartResponse<T>) -> Self {
        match value {
            UartResponse::Deny(response) => Err(response.into()),
            UartResponse::Normal(_) => Ok(()),
        }
    }
}

pub(crate) fn _uart_response_handler<T: IntoMqttMessage + TryFrom<AppData>>(
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
