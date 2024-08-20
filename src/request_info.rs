use std::any::Any;
use crate::MqttTopic;

// TODO 使用enum？
#[derive(Debug, Default)]
struct FrameKey {
    afn: u8,
    fn_num: u8,
}

#[derive(Debug)]
struct MqttReqInfo {
    topic: MqttTopic,
    token: String,
    extra_data: Option<Box<dyn Any>>,
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
    fn new(frame: &Frame, mqtt_req_info: Option<MqttReqInfo>) -> Self {
        ReqInfo {
            mqtt_req_info,
            frame_key: FrameKey {
                afn: frame.afn(),
                fn_num: frame.fn_num(),
            },
            seq_num: frame.seq_num(),
        }
    }
}

#[derive(Debug)]
pub struct UartMessage {
    req_info: ReqInfo,
    frame: Frame,
}

impl UartMessage {
    fn new(req_info: ReqInfo, frame: Frame) -> Self {
        UartMessage {
            req_info,
            frame,
        }
    }
}