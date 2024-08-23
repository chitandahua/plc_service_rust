use crate::protocol::Frame;
use crate::MqttTopic;
use std::any::Any;

// TODO 使用enum？
#[derive(Debug, Default, PartialEq, Eq)]
pub struct FrameKey(u8, u8);

impl FrameKey {
    pub fn new(afn: u8, fn_num: u8) -> Self {
        FrameKey(afn, fn_num)
    }

    pub fn afn(&self) -> u8 {
        self.0
    }

    pub fn fn_num(&self) -> u8 {
        self.1
    }

    pub fn to_tuple(&self) -> (u8, u8) {
        (self.0, self.1)
    }
}

#[derive(Debug)]
pub struct MqttReqInfo {
    topic: MqttTopic,
    token: String,
    extra_data: Option<Box<dyn Any + Send>>,
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

    pub fn topic(&self) -> &MqttTopic {
        &self.topic
    }

    pub fn token(&self) -> &str {
        &self.token
    }

    pub fn into_extra_data(self) -> Option<Box<dyn Any + Send>> {
        self.extra_data
    }
}

#[derive(Debug, Default)]
pub struct ReqInfo {
    mqtt_req_info: Option<MqttReqInfo>,
    frame_key: FrameKey,
    seq_num: u8,
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

    pub fn into_mqtt_req_info(self) -> Option<MqttReqInfo> {
        self.mqtt_req_info
    }

    pub fn frame_key(&self) -> &FrameKey {
        &self.frame_key
    }

    pub fn seq_num(&self) -> u8 {
        self.seq_num
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
