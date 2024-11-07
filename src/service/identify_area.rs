use crate::mqtt_handler::{MqttMsgHandler, MqttTopicType};
use crate::mqtt_message::{MqttMessage, MqttPayload, PayloadBody};
use crate::mqtt_topic::MqttTopic;
use crate::protocol::app_data::{
    ActiveNodeRegisterRequest, ConfirmResponse, IdentifyAreaSetRequest,
    ReportNodeInfoAndDeviceType, ReportWorkStatus, RunningStatusRequest, RunningStatusResponse,
    StopNodeRegisterRequest, WorkStatusType,
};
use crate::protocol::Frame;

use crate::service::parse_response::{mqtt_request_uart_handler, uart_response_mqtt_handler};
use crate::service::{IntoMqttMessage, UartResponse};

use crate::{MqttResponseError, ReqInfo, Result, UartMessage, APP_NAME};

use serde::{Deserialize, Serialize};
use std::sync::{mpsc, Arc, Condvar, Mutex};

struct IdentifyAreaInfo {
    app: Option<String>,
    result: Option<Result<()>>,
    finished: bool,
}

#[derive(Clone)]
pub struct IdentifyArea {
    info: Arc<Mutex<IdentifyAreaInfo>>,
    cond: Arc<Condvar>,
}

#[derive(Debug, Deserialize)]
struct MqttEnableSearchMeter {
    #[serde(rename = "starttime")]
    start_time: String,
    #[serde(rename = "schUnit")]
    unit: String,
    #[serde(rename = "schValue")]
    duration: u16,
    retry: u8,
    #[serde(rename = "slice")]
    random_wait_count: u8,
}

impl TryFrom<MqttEnableSearchMeter> for ActiveNodeRegisterRequest {
    type Error = crate::Error;
    fn try_from(request: MqttEnableSearchMeter) -> Result<Self> {
        Ok(Self::new(
            chrono::NaiveDateTime::parse_from_str(&request.start_time, "%Y-%m-%d %H:%M:%S")?,
            if request.unit == "hour" {
                request.duration * 60
            } else {
                request.duration
            },
            request.retry,
            request.random_wait_count,
        ))
    }
}

#[derive(Debug, Deserialize)]
struct MqttIdentifyAreaSetRequest {
    switch: u8,
}

impl IdentifyArea {
    pub fn new() -> Self {
        Self {
            info: Arc::new(Mutex::new(IdentifyAreaInfo {
                app: None,
                result: None,
                finished: false,
            })),
            cond: Arc::new(Condvar::new()),
        }
    }

    pub fn init(mqtt_msg_handler: &mut MqttMsgHandler) {
        use crate::config::SCHEMA_PATH;
        use crate::schema_check;
        let topic = format!("{}{}{}", "+/set/request/", APP_NAME, "/identiAreaCfg");
        let schema = schema_check::parse_schema(SCHEMA_PATH.join("identify_area_schema.json")).ok();
        mqtt_msg_handler.add_topic_filter(topic, MqttTopicType::IdentifyArea, schema);

        let topic = format!("{}{}{}", "+/set/request/", APP_NAME, "/enSearchMeter");
        let schema =
            schema_check::parse_schema(SCHEMA_PATH.join("enable_search_meter_schema.json")).ok();
        mqtt_msg_handler.add_topic_filter(topic, MqttTopicType::EnableSearchMeter, schema);

        let topic = format!("{}{}{}", "+/set/request/", APP_NAME, "/disSearchMeter");
        let schema =
            schema_check::parse_schema(SCHEMA_PATH.join("disable_search_meter_schema.json")).ok();
        mqtt_msg_handler.add_topic_filter(topic, MqttTopicType::DisableSearchMeter, schema);
    }
}

// 台区识别
impl IdentifyArea {
    pub fn mqtt_identify_area_set(
        message: MqttMessage,
        uart_msg_sender: &mpsc::Sender<UartMessage>,
    ) {
        let request: MqttIdentifyAreaSetRequest = serde_json::from_str(message.payload()).unwrap();
        mqtt_request_uart_handler::<IdentifyAreaSetRequest>(
            IdentifyAreaSetRequest::new(request.switch),
            message,
            uart_msg_sender,
        );
    }

    pub fn uart_identify_area_set(
        message: UartMessage,
        mqtt_msg_sender: &mpsc::Sender<MqttMessage>,
    ) -> Result<()> {
        uart_response_mqtt_handler::<ConfirmResponse>(message, mqtt_msg_sender)
    }
}

// 搜表(从节点注册)
impl IdentifyArea {
    pub fn mqtt_active_slave_node_register(
        &self,
        message: MqttMessage,
        mqtt_msg_sender: &mpsc::Sender<MqttMessage>,
        uart_msg_sender: &mpsc::Sender<UartMessage>,
    ) -> Result<()> {
        let request: MqttEnableSearchMeter = serde_json::from_str(message.payload()).unwrap();
        let mqtt_req_info = message.to_mqtt_req_info();
        let app = MqttTopic::get_app(message.topic()).to_string();
        mqtt_request_uart_handler::<ActiveNodeRegisterRequest>(
            ActiveNodeRegisterRequest::try_from(request)?,
            message,
            uart_msg_sender,
        );

        // 等待回复
        let mut info = self.info.lock().unwrap();
        info.finished = false;
        info.app = Some(app);
        info = self
            .cond
            .wait_while(info, |info| info.result.is_none())
            .unwrap();
        let result = info.result.take().unwrap();
        let is_err = result.is_err();
        mqtt_msg_sender
            .send(result.into_mqtt_message(mqtt_req_info))
            .unwrap();
        if is_err {
            info.app.take();
            return Err(anyhow::anyhow!("enable search meter failed"));
        }

        // 等待上报结束 or 超时
        // TODO 工作标志若为仍在工作中 则继续等待？
        while !info.finished {
            let result = self
                .cond
                .wait_timeout_while(info, std::time::Duration::from_secs(10 * 60), |info| {
                    info.result.is_none()
                })
                .unwrap();
            info = result.0;
            if result.1.timed_out() {
                let frame = Frame::new_request(None, RunningStatusRequest);
                let req_info = ReqInfo::new(&frame, None);
                uart_msg_sender
                    .send(UartMessage::new(req_info, frame))
                    .unwrap();

                // 等待回复
                info = self
                    .cond
                    .wait_while(info, |info| info.result.is_none())
                    .unwrap();
                if let Err(e) = info.result.take().unwrap() {
                    tracing::error!(cause = ?e, "get running status failed");
                }
            } else {
                info.result = None;
            }
        }

        info.app.take();
        tracing::info!("enable search meter finished");
        Ok(())
    }

    pub fn notify(&self, result: Result<()>) {
        let mut info = self.info.lock().unwrap();
        info.result = Some(result);
        self.cond.notify_one();
    }

    pub fn notify_finished(&self, result: Result<()>) {
        let mut info = self.info.lock().unwrap();
        info.result = Some(result);
        info.finished = true;
        self.cond.notify_one();
    }

    pub fn uart_active_slave_node_register_timeout(&self) {
        let mut info = self.info.lock().unwrap();
        info.result = Some(Err(anyhow::anyhow!(MqttResponseError::Timeout)));
        self.cond.notify_one();
    }

    pub fn mqtt_stop_slave_node_register(
        message: MqttMessage,
        uart_msg_sender: &mpsc::Sender<UartMessage>,
    ) {
        mqtt_request_uart_handler::<StopNodeRegisterRequest>(
            StopNodeRegisterRequest,
            message,
            uart_msg_sender,
        )
    }

    pub fn uart_active_slave_node_register(&self, message: UartMessage) -> Result<()> {
        let response = UartResponse::<ConfirmResponse>::try_from(message.frame)?;
        self.notify(response.into());

        Ok(())
    }

    pub fn uart_search_meter_running_status(&self, message: UartMessage) -> Result<()> {
        let response = UartResponse::<RunningStatusResponse>::try_from(message.frame)?;
        let result: Result<RunningStatusResponse> = response.into();

        match result {
            Ok(status) => {
                if status.work_status.work_status == 0 {
                    self.notify_finished(Ok(()))
                } else {
                    self.notify(Ok(()))
                }
            }
            Err(err) => self.notify(Err(err)),
        }

        Ok(())
    }

    pub fn uart_stop_slave_node_register(
        message: UartMessage,
        mqtt_msg_sender: &mpsc::Sender<MqttMessage>,
    ) -> Result<()> {
        uart_response_mqtt_handler::<ConfirmResponse>(message, mqtt_msg_sender)
    }
}

// 搜表上报
#[derive(Debug, Serialize)]
struct MqttSearchMeterNodeInfo {
    #[serde(rename = "acqAddr")]
    acq_addr: String,
    #[serde(rename = "proType")]
    pro_type: u8,
    #[serde(rename = "deviceType")]
    device_type: u8,
}

#[derive(Debug, Serialize)]
struct MqttSearchMeterResponse(Vec<MqttSearchMeterNodeInfo>);

impl From<ReportNodeInfoAndDeviceType> for MqttSearchMeterResponse {
    fn from(value: ReportNodeInfoAndDeviceType) -> Self {
        MqttSearchMeterResponse(vec![MqttSearchMeterNodeInfo {
            acq_addr: value.node_info.address.to_string(),
            pro_type: value.node_info.protocol_type,
            device_type: value.device_type,
        }])
    }
}

impl IdentifyArea {
    pub fn uart_slave_node_info_report(
        &self,
        _message: UartMessage,
        _mqtt_msg_sender: &mpsc::Sender<MqttMessage>,
        _uart_msg_sender: &mpsc::Sender<UartMessage>,
    ) -> Result<()> {
        todo!()
    }

    pub fn uart_slave_work_status_report(
        &self,
        message: UartMessage,
        uart_msg_sender: &mpsc::Sender<UartMessage>,
    ) -> Result<()> {
        // 回复
        let frame = Frame::new_response(message.frame.get_seq(), None, ConfirmResponse::default());
        let response = UartMessage::new(ReqInfo::new(&message.frame, None), frame);
        uart_msg_sender.send(response).unwrap();

        let work_status = ReportWorkStatus::try_from(message.frame.into_app_data())?;
        // 搜表结束通知
        if work_status.work_status_type == WorkStatusType::Search {
            self.notify_finished(Ok(()));
        }
        Ok(())
    }

    pub fn uart_slave_node_info_and_device_report(
        &self,
        message: UartMessage,
        mqtt_msg_sender: &mpsc::Sender<MqttMessage>,
        uart_msg_sender: &mpsc::Sender<UartMessage>,
    ) -> Result<()> {
        // 回复
        let frame = Frame::new_response(message.frame.get_seq(), None, ConfirmResponse::default());
        let response = UartMessage::new(ReqInfo::new(&message.frame, None), frame);
        uart_msg_sender.send(response).unwrap();

        let app = {
            let info = self.info.lock().unwrap();
            info.app.clone().ok_or(anyhow::anyhow!("no request app"))?
        };
        let topic = format!("{}/notify/spont/{}/searchMeter", APP_NAME, app);
        let report = ReportNodeInfoAndDeviceType::try_from(message.frame.into_app_data())?;
        let payload = MqttSearchMeterResponse::from(report);
        mqtt_msg_sender
            .send(MqttMessage::new(
                topic,
                MqttPayload::new_with_body(Some(PayloadBody::Nested {
                    body: serde_json::to_value(payload).unwrap(),
                })),
            ))
            .unwrap();
        self.notify(Ok(()));

        Ok(())
    }
}
