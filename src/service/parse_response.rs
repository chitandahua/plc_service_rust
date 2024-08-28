use serde_json::Value;

use crate::mqtt_message::{MqttMessage, MqttPayload};
use crate::protocol::app_data::{ConfirmResponse, DenyResponse};
use crate::protocol::AppData;
use crate::protocol::Frame;
use crate::request_info::MqttReqInfo;
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
        if let Ok(response) = T::try_from(frame.clone().into_app_data()) {
            Ok(UartResponse::Normal(response))
        } else if let Ok(response) = DenyResponse::try_from(frame.into_app_data()) {
            Ok(UartResponse::Deny(response))
        } else {
            anyhow::bail!("invalid response frame")
        }
    }
}

pub trait IntoMqttMessage {
    fn into_mqtt_message(self, mqtt_req_info: MqttReqInfo) -> MqttMessage;
}

impl IntoMqttMessage for ConfirmResponse {
    fn into_mqtt_message(self, mqtt_req_info: MqttReqInfo) -> MqttMessage {
        let payload = MqttPayload::new_with_token_status_reason(mqtt_req_info.token(), "OK", "OK");
        MqttMessage::new(mqtt_req_info.topic().topic_transfer(), payload)
    }
}

impl IntoMqttMessage for DenyResponse {
    fn into_mqtt_message(self, mqtt_req_info: MqttReqInfo) -> MqttMessage {
        let payload = MqttPayload::new_with_token_status_reason(
            mqtt_req_info.token(),
            "FAILURE",
            self.error_code(),
        );
        MqttMessage::new(mqtt_req_info.topic().topic_transfer(), payload)
    }
}
