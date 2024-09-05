use std::sync::atomic::AtomicU64;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::request_info::MqttReqInfo;
use crate::MqttTopic;
use crate::Result;

#[derive(Debug, Serialize, Deserialize, strum_macros::EnumString, strum_macros::Display)]
pub enum Status {
    #[strum(serialize = "OK")]
    #[serde(rename = "OK")]
    Success,
    #[serde(rename = "FAILURE")]
    Failure,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum PayloadBody {
    Flat(Value),
    Nested { body: Value },
}

pub const SUCCESS: &str = "OK";

#[derive(Debug, Serialize, Deserialize)]
pub struct MqttPayload {
    token: String,
    timestamp: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    status: Option<Status>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<String>,
    #[serde(flatten, skip_serializing_if = "Option::is_none")]
    body: Option<PayloadBody>,
}

static TOKEN: AtomicU64 = AtomicU64::new(0);

impl MqttPayload {
    pub fn new_with_body(body: Option<PayloadBody>) -> Self {
        Self {
            token: AtomicU64::fetch_add(&TOKEN, 1, std::sync::atomic::Ordering::Relaxed)
                .to_string(),
            timestamp: get_timestamp(),
            status: None,
            reason: None,
            body,
        }
    }

    pub fn new_with_token_result(token: impl ToString, result: Result<()>) -> Self {
        match result {
            Ok(_) => Self::new_with_token(token, None),
            Err(e) => Self::new(token, Status::Failure, e, None),
        }
    }

    pub fn new_with_status_reason(status: Status, reason: impl ToString) -> Self {
        Self {
            token: AtomicU64::fetch_add(&TOKEN, 1, std::sync::atomic::Ordering::Relaxed)
                .to_string(),
            timestamp: get_timestamp(),
            status: Some(status),
            reason: Some(reason.to_string()),
            body: None,
        }
    }

    pub fn new_with_token(token: impl ToString, body: Option<PayloadBody>) -> Self {
        Self {
            token: token.to_string(),
            timestamp: get_timestamp(),
            status: Some(Status::Success),
            reason: Some(SUCCESS.to_string()),
            body,
        }
    }

    pub fn new(
        token: impl ToString,
        status: Status,
        reason: impl ToString,
        body: Option<PayloadBody>,
    ) -> Self {
        Self {
            token: token.to_string(),
            timestamp: get_timestamp(),
            status: Some(status),
            reason: Some(reason.to_string()),
            body,
        }
    }

    pub fn into_body(self) -> Option<PayloadBody> {
        self.body
    }

    pub fn token(&self) -> &str {
        self.token.as_str()
    }
}

fn get_timestamp() -> String {
    // 获取utc时间 ISO 8601 格式
    chrono::Local::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, false)
}

impl std::fmt::Display for MqttPayload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}

impl From<MqttPayload> for Value {
    fn from(msg: MqttPayload) -> Self {
        serde_json::to_value(msg).unwrap()
    }
}

#[derive(Debug)]
pub struct MqttMessage {
    topic: String,
    payload: String,
}

impl MqttMessage {
    pub fn new(topic: impl ToString, payload: impl ToString) -> Self {
        Self {
            topic: topic.to_string(),
            payload: payload.to_string(),
        }
    }

    pub fn new_with_msg_body(message: MqttMessage, body: Option<PayloadBody>) -> Self {
        let topic: MqttTopic = message.topic().try_into().unwrap();
        let payload = MqttPayload::new_with_token(message.get_token(), body);

        Self::new(topic.topic_transfer(), payload)
    }

    pub fn new_with_req_info_body(mqtt_req_info: MqttReqInfo, body: Option<PayloadBody>) -> Self {
        let payload = MqttPayload::new_with_token(mqtt_req_info.token(), body);
        Self::new(mqtt_req_info.topic(), payload)
    }

    pub fn new_with_msg_status_reason(
        message: MqttMessage,
        status: Status,
        reason: impl ToString,
    ) -> Self {
        let topic: MqttTopic = message.topic().try_into().unwrap();
        let payload = MqttPayload::new(message.get_token(), status, reason.to_string(), None);

        Self::new(topic.topic_transfer(), payload)
    }

    pub fn new_with_req_info_status_reason(
        mqtt_req_info: MqttReqInfo,
        status: Status,
        reason: impl ToString,
    ) -> Self {
        let payload = MqttPayload::new(mqtt_req_info.token(), status, reason.to_string(), None);
        Self::new(mqtt_req_info.topic(), payload)
    }

    pub fn topic(&self) -> &str {
        self.topic.as_str()
    }

    pub fn payload(&self) -> &str {
        self.payload.as_str()
    }

    pub fn get_priority(&self) -> u64 {
        // payload转json 获取prio字段 默认0
        let payload: Value = serde_json::from_str(self.payload()).unwrap();
        payload["prio"].as_u64().unwrap_or(0)
    }

    pub fn get_token(&self) -> String {
        // payload转json 获取token字段
        let payload: Value = serde_json::from_str(self.payload()).unwrap();
        payload["token"].as_str().unwrap().to_owned()
    }

    pub fn to_mqtt_req_info(&self) -> MqttReqInfo {
        MqttReqInfo::new(self.topic(), self.get_token().to_string(), None)
    }
}

impl TryFrom<paho_mqtt::Message> for MqttMessage {
    type Error = crate::Error;

    fn try_from(msg: paho_mqtt::Message) -> Result<Self> {
        let payload = String::from_utf8(msg.payload().to_vec())?;
        Ok(MqttMessage::new(msg.topic(), payload))
    }
}

#[cfg(test)]
mod mqtt_message_tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn test_mqtt_payload() {
        let payload = MqttPayload::new(json!({}));
        assert_eq!(payload.body(), json!({}));

        let payload = MqttPayload::new_with_token("123", Value::Null);
        assert_eq!(payload.token(), "123");
        assert_eq!(payload.body(), Value::Null);
    }

    #[test]
    fn test_mqtt_message() {
        let msg = MqttMessage::new("topic", "payload");
        assert_eq!(msg.topic(), "topic");
        assert_eq!(msg.payload(), "payload");
    }
}
