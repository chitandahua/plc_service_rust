use crate::protocol::Frame;
use crate::MqttTopic;
use std::any::Any;

// TODO 使用enum？
#[derive(Debug, Default)]
pub struct FrameKey(pub u8, pub u8);

#[derive(Debug)]
pub struct MqttReqInfo {
    pub topic: MqttTopic,
    pub token: String,
    pub extra_data: Option<Box<dyn Any + Send>>,
}

impl MqttReqInfo {
    pub fn new(topic: String, token: String) -> Self {
        MqttReqInfo {
            topic: MqttTopic::try_from(topic.as_str()).unwrap(),
            token,
            extra_data: None,
        }
    }

    pub fn new_with_data(topic: String, token: String, extra_data: Box<dyn Any + Send>) -> Self {
        MqttReqInfo {
            topic: MqttTopic::try_from(topic.as_str()).unwrap(),
            token,
            extra_data: Some(extra_data),
        }
    }
}

#[derive(Debug, Default)]
pub struct ReqInfo {
    pub mqtt_req_info: Option<MqttReqInfo>,
    pub frame_key: FrameKey,
    pub seq_num: u8,
    // 超时回调？ TODO
    // timeout_cb: Box<dyn Fn() + Send>,
}

impl ReqInfo {
    pub fn new(frame: &Frame) -> Self {
        ReqInfo {
            mqtt_req_info: None,
            frame_key: FrameKey(frame.afn().into(), frame.fn_num()),
            seq_num: frame.get_seq(),
        }
    }

    pub fn new_with_mqtt(
        frame: &Frame,
        topic: impl Into<String>,
        token: impl Into<String>,
    ) -> Self {
        ReqInfo {
            mqtt_req_info: Some(MqttReqInfo::new(topic.into(), token.into())),
            frame_key: FrameKey(frame.afn().into(), frame.fn_num()),
            seq_num: frame.get_seq(),
        }
    }

    pub fn is_init(&self) -> bool {
        self.mqtt_req_info.is_none()
    }
}

#[derive(Debug)]
pub struct UartMessage {
    pub req_info: ReqInfo,
    pub frame: Frame,
}

impl UartMessage {
    pub fn new(req_info: ReqInfo, frame: Frame) -> Self {
        UartMessage { req_info, frame }
    }
}
