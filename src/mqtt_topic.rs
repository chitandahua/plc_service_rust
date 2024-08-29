use crate::{Result, APP_NAME};
use std::fmt::{Display, Formatter};
use thiserror::Error;
use tracing::debug;

#[derive(Debug, PartialEq)]
pub enum MqttTopicOperator {
    Set,
    Get,
    Action,
    Notify,
}

impl TryFrom<&str> for MqttTopicOperator {
    type Error = crate::Error;
    fn try_from(value: &str) -> Result<MqttTopicOperator> {
        // 将字符串转换为MqttTopicOperator
        match value {
            "set" => Ok(MqttTopicOperator::Set),
            "get" => Ok(MqttTopicOperator::Get),
            "action" => Ok(MqttTopicOperator::Action),
            "notify" => Ok(MqttTopicOperator::Notify),
            _ => Err(TopicError::Operator(value.to_string()).into()),
        }
    }
}

impl Display for MqttTopicOperator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            MqttTopicOperator::Set => write!(f, "set"),
            MqttTopicOperator::Get => write!(f, "get"),
            MqttTopicOperator::Action => write!(f, "action"),
            MqttTopicOperator::Notify => write!(f, "notify"),
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum MqttTopicInfoType {
    Request,
    Response,
    Event,
    Spont,
}

impl TryFrom<&str> for MqttTopicInfoType {
    type Error = crate::Error;
    fn try_from(value: &str) -> Result<MqttTopicInfoType> {
        // 将字符串转换为MqttTopicInfoType
        match value {
            "request" => Ok(MqttTopicInfoType::Request),
            "response" => Ok(MqttTopicInfoType::Response),
            "event" => Ok(MqttTopicInfoType::Event),
            "spont" => Ok(MqttTopicInfoType::Spont),
            _ => Err(TopicError::InfoType(value.to_string()).into()),
        }
    }
}

impl Display for MqttTopicInfoType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            MqttTopicInfoType::Request => write!(f, "request"),
            MqttTopicInfoType::Response => write!(f, "response"),
            MqttTopicInfoType::Event => write!(f, "event"),
            MqttTopicInfoType::Spont => write!(f, "spont"),
        }
    }
}

pub const _WILDCARD_AST: &str = "#";
pub const _WILDCARD_PLUS: &str = "+";

#[derive(Debug)]
// {app}/{operator}/{info_type}/{info_target}/{info_object}
pub(crate) struct MqttTopic {
    app: String,
    operator: MqttTopicOperator,
    info_type: MqttTopicInfoType,
    info_target: String,
    info_object: String,
}

#[derive(Error, Debug, PartialEq)]
pub(crate) enum TopicError {
    #[error("invalid operator {0}")]
    Operator(String),
    #[error("invalid info type {0}")]
    InfoType(String),
    #[error("invalid info target {0}")]
    InfoTarget(String),
    #[error("invalid topic {0}")]
    Topic(String),
}

impl TryFrom<&str> for MqttTopic {
    type Error = crate::Error;
    fn try_from(value: &str) -> Result<MqttTopic> {
        // 将字符串转换为MqttTopic
        let parts: Vec<&str> = value.split('/').collect();
        anyhow::ensure!(parts.len() == 5, TopicError::Topic(value.to_string()));
        // 判断info_target是否为APP_NAME
        //anyhow::ensure!(
        //    parts[3] == APP_NAME,
        //    TopicError::InfoTarget(parts[3].to_string())
        //);

        // 将字符串转换为MqttTopic
        Ok(MqttTopic {
            app: parts[0].to_string(),
            operator: parts[1].try_into()?,
            info_type: parts[2].try_into()?,
            info_target: parts[3].to_string(),
            info_object: parts[4].to_string(),
        })
    }
}

impl Display for MqttTopic {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}/{}/{}/{}/{}",
            self.app, self.operator, self.info_type, self.info_target, self.info_object
        )
    }
}

// {app}/{operator}/{info_type}/{info_target}/{info_object}
// 转成
// {info_target}/{operator}/{info_type}/{app}/{info_object}
#[allow(dead_code)]
impl MqttTopic {
    fn _new(
        app: impl Into<String>,
        operator: MqttTopicOperator,
        info_type: MqttTopicInfoType,
        info_target: impl Into<String>,
        info_object: impl Into<String>,
    ) -> Self {
        Self {
            app: app.into(),
            operator,
            info_type,
            info_target: info_target.into(),
            info_object: info_object.into(),
        }
    }

    pub fn app(&self) -> &str {
        self.app.as_str()
    }

    fn operator(&self) -> &MqttTopicOperator {
        &self.operator
    }

    pub fn info_target(&self) -> &str {
        self.info_target.as_str()
    }

    fn info_object(&self) -> &str {
        self.info_object.as_str()
    }

    fn topic_app_suffix(&self) -> String {
        format!(
            "{}/{}/{}/{}",
            self.operator, self.info_type, self.info_target, self.info_object
        )
    }

    // request转成response
    pub fn topic_transfer(&self) -> String {
        match self.info_type {
            MqttTopicInfoType::Request => format!(
                "{}/{}/{}/{}/{}",
                self.info_target,
                self.operator,
                MqttTopicInfoType::Response,
                self.app,
                self.info_object
            ),
            _ => format!(
                "{}/{}/{}/{}/{}",
                self.info_target, self.operator, self.info_type, self.app, self.info_object
            ),
        }
    }

    pub fn transfer(topic: &str) -> String {
        MqttTopic::try_from(topic).unwrap().topic_transfer()
    }
}

#[cfg(test)]
mod topic_tests {
    use super::*;

    #[test]
    fn test_topic_info_type() {
        assert!(
            TryInto::<MqttTopicInfoType>::try_into("request").unwrap()
                == MqttTopicInfoType::Request
        );
        assert!(
            TryInto::<MqttTopicInfoType>::try_into("response").unwrap()
                == MqttTopicInfoType::Response
        );
        assert!(
            TryInto::<MqttTopicInfoType>::try_into("event").unwrap() == MqttTopicInfoType::Event
        );
        assert!(
            TryInto::<MqttTopicInfoType>::try_into("spont").unwrap() == MqttTopicInfoType::Spont
        );

        assert!(MqttTopicInfoType::Request.to_string() == "request");
        assert!(MqttTopicInfoType::Response.to_string() == "response");
        assert!(MqttTopicInfoType::Event.to_string() == "event");
        assert!(MqttTopicInfoType::Spont.to_string() == "spont");
    }

    #[test]
    fn test_topic_operator() {
        assert!(TryInto::<MqttTopicOperator>::try_into("set").unwrap() == MqttTopicOperator::Set);
        assert!(TryInto::<MqttTopicOperator>::try_into("get").unwrap() == MqttTopicOperator::Get);
        assert!(
            TryInto::<MqttTopicOperator>::try_into("action").unwrap() == MqttTopicOperator::Action
        );
        assert!(
            TryInto::<MqttTopicOperator>::try_into("notify").unwrap() == MqttTopicOperator::Notify
        );

        assert!(MqttTopicOperator::Set.to_string() == "set");
        assert!(MqttTopicOperator::Get.to_string() == "get");
        assert!(MqttTopicOperator::Action.to_string() == "action");
        assert!(MqttTopicOperator::Notify.to_string() == "notify");
    }

    #[test]
    fn test_topic() {
        let topic_str = format!("app/set/request/{APP_NAME}/123");
        let topic: MqttTopic = topic_str.as_str().try_into().unwrap();
        assert!(topic.app() == "app");
        assert_eq!(topic.operator, MqttTopicOperator::Set);
        assert_eq!(topic.info_type, MqttTopicInfoType::Request);
        assert!(topic.info_target == APP_NAME);
        assert!(topic.info_object() == "123");

        assert_eq!(topic.to_string(), topic_str);

        let topic_str = format!("app/get/response/{APP_NAME}/123");
        let topic: MqttTopic = topic_str.as_str().try_into().unwrap();
        assert!(topic.app() == "app");
        assert_eq!(topic.operator, MqttTopicOperator::Get);
        assert_eq!(topic.info_type, MqttTopicInfoType::Response);
        assert!(topic.info_target == APP_NAME);
        assert!(topic.info_object() == "123");

        assert_eq!(topic.to_string(), topic_str);
    }

    #[test]
    fn test_topic_transfer() {
        let topic_str = format!("app/get/response/{APP_NAME}/123");
        let topic: MqttTopic = topic_str.as_str().try_into().unwrap();
        assert!(topic.topic_transfer() == format!("{APP_NAME}/get/response/app/123"));

        let topic_str = format!("app/notify/spont/{APP_NAME}/123");
        let topic: MqttTopic = topic_str.as_str().try_into().unwrap();
        assert!(topic.topic_transfer() == format!("{APP_NAME}/notify/spont/app/123"));
    }

    #[test]
    fn test_topic_app_suffix() {
        let topic_str = format!("app/get/response/{APP_NAME}/123");
        let topic: MqttTopic = topic_str.as_str().try_into().unwrap();
        assert!(topic.topic_app_suffix() == format!("get/response/{APP_NAME}/123"));
    }

    #[test]
    fn test_topic_invalid() {
        assert_eq!(
            TryInto::<MqttTopic>::try_into("app/set/request/device")
                .unwrap_err()
                .downcast::<TopicError>()
                .unwrap(),
            TopicError::Topic("app/set/request/device".to_string())
        );

        assert_eq!(
            TryInto::<MqttTopic>::try_into("app/set/")
                .unwrap_err()
                .downcast::<TopicError>()
                .unwrap(),
            TopicError::Topic("app/set/".to_string())
        );

        assert_eq!(
            TryInto::<MqttTopic>::try_into("app/set/request/device/123")
                .unwrap_err()
                .downcast::<TopicError>()
                .unwrap(),
            TopicError::InfoTarget("device".to_string())
        );

        assert_eq!(
            TryInto::<MqttTopic>::try_into(format!("app/test/request/{APP_NAME}/123").as_str())
                .unwrap_err()
                .downcast::<TopicError>()
                .unwrap(),
            TopicError::Operator("test".to_string())
        );

        assert_eq!(
            TryInto::<MqttTopic>::try_into(format!("app/set/test/{APP_NAME}/123").as_str())
                .unwrap_err()
                .downcast::<TopicError>()
                .unwrap(),
            TopicError::InfoType("test".to_string())
        );
    }
}
