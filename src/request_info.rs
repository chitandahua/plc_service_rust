use crate::protocol::app_data::Afn;
use crate::protocol::Frame;
use crate::MqttTopic;
use std::any::Any;

// TODO 使用enum？
#[derive(Debug, PartialEq, Eq)]
pub struct FrameKey(Afn, u8);

impl FrameKey {
    pub fn new(afn: Afn, fn_num: u8) -> Self {
        FrameKey(afn, fn_num)
    }

    pub fn afn(&self) -> Afn {
        self.0
    }

    pub fn fn_num(&self) -> u8 {
        self.1
    }

    pub fn to_tuple(&self) -> (Afn, u8) {
        (self.0, self.1)
    }
}

#[derive(Debug)]
pub struct MqttReqInfo {
    topic: String, // 回复的topic
    token: String,
    extra_data: Option<Box<dyn Any + Send + Sync>>,
}

impl MqttReqInfo {
    pub fn new(
        topic: &str,
        token: impl ToString,
        extra_data: Option<Box<dyn Any + Send + Sync>>,
    ) -> Self {
        MqttReqInfo {
            topic: MqttTopic::transfer(topic),
            token: token.to_string(),
            extra_data,
        }
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub fn token(&self) -> &str {
        &self.token
    }

    pub fn extra_data(&self) -> Option<&Box<dyn Any + Send + Sync>> {
        self.extra_data.as_ref()
    }

    pub fn get_extra_data(&mut self) -> Option<Box<dyn Any + Send + Sync>> {
        self.extra_data.take()
    }

    pub fn set_extra_data(&mut self, extra_data: Option<Box<dyn Any + Send + Sync>>) {
        self.extra_data = extra_data;
    }
}

#[derive(Debug)]
pub struct ReqInfo {
    mqtt_req_info: Option<MqttReqInfo>,
    frame_key: FrameKey,
    seq_num: u8,
}

impl ReqInfo {
    pub fn new(frame: &Frame, mqtt_req_info: Option<MqttReqInfo>) -> Self {
        ReqInfo {
            mqtt_req_info,
            frame_key: FrameKey::new(frame.afn(), frame.fn_num()),
            seq_num: frame.get_seq(),
        }
    }

    pub fn new_with_mqtt(
        frame: &Frame,
        topic: impl AsRef<str>,
        token: impl ToString,
        extra_data: Option<Box<dyn Any + Send + Sync>>,
    ) -> Self {
        ReqInfo {
            mqtt_req_info: Some(MqttReqInfo::new(
                topic.as_ref(),
                token.to_string(),
                extra_data,
            )),
            frame_key: FrameKey::new(frame.afn(), frame.fn_num()),
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

    pub fn mqtt_req_info(&self) -> Option<&MqttReqInfo> {
        self.mqtt_req_info.as_ref()
    }
}

pub struct UartMessage {
    pub req_info: ReqInfo,
    pub frame: Frame,
}

impl UartMessage {
    pub fn new(req_info: ReqInfo, frame: Frame) -> Self {
        UartMessage { req_info, frame }
    }
}
