use crate::mqtt_handler::{MqttMsgHandler, MqttTopicType};
use crate::mqtt_message::{MqttMessage, MqttPayload, PayloadBody};
use crate::mqtt_topic::MqttTopic;
use crate::protocol::app_data::{
    ActiveNodeRegisterRequest, Afn, ConfirmResponse, IdentifyAreaSetRequest,
    ReportNodeInfoAndDeviceType, ReportWorkStatus, RouteSet, RunningStatusRequest,
    RunningStatusResponse, StopNodeRegisterRequest, WorkStatusType,
};
use crate::protocol::Frame;
use crate::request_info::FrameKey;

use crate::service::parse_response::{
    mqtt_request_handler, mqtt_request_uart_handler, uart_response_handler,
};
use crate::service::{IntoMqttMessage, RouteCtrl, UartResponse};

use crate::{
    register_mqtt_request_topics, MqttResponseError, ReqInfo, Result, UartMessage, APP_NAME,
};

use serde::{Deserialize, Serialize};
use std::sync::{mpsc, Arc, Condvar, Mutex};
use threadpool::ThreadPool;

#[derive(Debug, PartialEq)]
enum IdentifyAreaState {
    Running,
    Finished,
    Canceled,
    None,
}

struct IdentifyAreaInfo {
    report_info: Vec<ReportNodeInfoAndDeviceType>,
    result: Option<Result<()>>,
    state: IdentifyAreaState,
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

impl From<MqttIdentifyAreaSetRequest> for IdentifyAreaSetRequest {
    fn from(req: MqttIdentifyAreaSetRequest) -> Self {
        Self::new(req.switch)
    }
}

impl IdentifyArea {
    pub fn new() -> Self {
        Self {
            info: Arc::new(Mutex::new(IdentifyAreaInfo {
                report_info: Vec::new(),
                result: None,
                state: IdentifyAreaState::None,
            })),
            cond: Arc::new(Condvar::new()),
        }
    }

    pub fn init(mqtt_msg_handler: &mut MqttMsgHandler) {
        register_mqtt_request_topics!(
            mqtt_msg_handler,
            (
                "set",
                "identiAreaCfg",
                MqttTopicType::IdentifyArea,
                "identify_area_schema.json"
            ),
            (
                "set",
                "enSearchMeter",
                MqttTopicType::EnableSearchMeter,
                "enable_search_meter_schema.json"
            ),
            (
                "set",
                "disSearchMeter",
                MqttTopicType::DisableSearchMeter,
                "disable_search_meter_schema.json"
            ),
        )
    }
}

// 台区识别
impl IdentifyArea {
    pub fn mqtt_identify_area_set(
        message: MqttMessage,
        uart_msg_sender: &mpsc::Sender<UartMessage>,
    ) {
        mqtt_request_handler::<IdentifyAreaSetRequest, MqttIdentifyAreaSetRequest>(
            message,
            uart_msg_sender,
        );
    }

    pub fn uart_identify_area_set(
        message: UartMessage,
        mqtt_msg_sender: &mpsc::Sender<MqttMessage>,
    ) -> Result<()> {
        uart_response_handler::<ConfirmResponse, ()>(message, mqtt_msg_sender)
    }
}

// 搜表(从节点注册)
impl IdentifyArea {
    pub fn mqtt_active_slave_node_register(
        &self,
        route_ctrl: &RouteCtrl,
        message: MqttMessage,
        mqtt_msg_sender: &mpsc::Sender<MqttMessage>,
        uart_msg_sender: &mpsc::Sender<UartMessage>,
        thread_pool: &ThreadPool,
    ) -> Result<()> {
        let request: MqttEnableSearchMeter = serde_json::from_str(message.payload()).unwrap();
        let mqtt_req_info = message.to_mqtt_req_info();
        let report_topic = format!(
            "{}/notify/spont/{}/searchMeter",
            APP_NAME,
            MqttTopic::get_app(message.topic())
        );

        let request = match ActiveNodeRegisterRequest::try_from(request) {
            Ok(request) => {
                route_ctrl.auto_pause_metering(uart_msg_sender)?;
                request
            }
            Err(e) => {
                mqtt_msg_sender
                    .send(
                        anyhow::anyhow!(MqttResponseError::InvalidJson(e.to_string()))
                            .into_mqtt_message(mqtt_req_info),
                    )
                    .unwrap();
                return Err(e);
            }
        };

        thread_pool.execute({
            let identify_area = self.clone();
            let mqtt_msg_sender = mqtt_msg_sender.clone();
            let uart_msg_sender = uart_msg_sender.clone();
            move || {
                // 等待回复
                let mut info = identify_area.info.lock().unwrap();
                if info.state == IdentifyAreaState::Running {
                    mqtt_msg_sender
                        .send(
                            anyhow::anyhow!("search meter already running")
                                .into_mqtt_message(mqtt_req_info),
                        )
                        .unwrap();
                    return;
                }
                info.state = IdentifyAreaState::Running;
                mqtt_request_uart_handler::<ActiveNodeRegisterRequest>(
                    request,
                    message,
                    &uart_msg_sender,
                );

                info = identify_area
                    .cond
                    .wait_while(info, |info| info.result.is_none())
                    .unwrap();
                let result = info.result.take().unwrap();
                let err = result.as_ref().err().map(|e| e.to_string());
                mqtt_msg_sender
                    .send(result.into_mqtt_message(mqtt_req_info))
                    .unwrap();
                if err.is_some() {
                    tracing::error!(cause = ?err, "active slave node register failed");
                    info.state = IdentifyAreaState::None;
                    return;
                }

                // 等待上报结束 or 超时
                // TODO 工作标志若为仍在工作中 则继续等待？
                while info.state == IdentifyAreaState::Running {
                    let result = identify_area
                        .cond
                        .wait_timeout_while(info, std::time::Duration::from_secs(10 * 60), |info| {
                            info.report_info.is_empty() && info.result.is_none()
                        })
                        .unwrap();
                    info = result.0;
                    if result.1.timed_out() {
                        let frame = Frame::new_request(None, RunningStatusRequest);
                        let req_info = ReqInfo::new_with_origin_req_key(
                            &frame,
                            FrameKey::new(Afn::RouteSet, RouteSet::ActiveNodeRegister as u8),
                        );
                        uart_msg_sender
                            .send(UartMessage::new(req_info, frame))
                            .unwrap();

                        // 等待回复
                        info = identify_area
                            .cond
                            .wait_while(info, |info| info.result.is_none())
                            .unwrap();
                        if let Err(e) = info.result.take().unwrap() {
                            tracing::error!(cause = ?e, "get running status failed");
                        }
                    } else if !info.report_info.is_empty() {
                        let body = std::mem::take(&mut info.report_info)
                            .into_iter()
                            .map(MqttSearchMeterResponse::from)
                            .collect::<Vec<_>>();
                        mqtt_msg_sender
                            .send(MqttMessage::new(
                                report_topic.clone(),
                                MqttPayload::new_with_body(Some(PayloadBody::Nested {
                                    body: serde_json::to_value(body).unwrap(),
                                })),
                            ))
                            .unwrap();
                    } else {
                        info.result = None;
                    }
                }

                tracing::info!(
                    "search meter {}",
                    if info.state == IdentifyAreaState::Finished {
                        "finished"
                    } else {
                        "canceled"
                    }
                );
                info.state = IdentifyAreaState::None;
            }
        });

        Ok(())
    }

    pub fn notify(&self, result: Result<()>) {
        let mut info = self.info.lock().unwrap();
        info.result = Some(result);
        self.cond.notify_one();
    }

    fn notify_state(&self, result: Result<()>, state: IdentifyAreaState) {
        let mut info = self.info.lock().unwrap();
        info.result = Some(result);
        info.state = state;
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
                if status.running_status.work_flag == 0 {
                    tracing::debug!("search meter finished");
                    self.notify_state(Ok(()), IdentifyAreaState::Finished);
                } else {
                    self.notify(Ok(()))
                }
            }
            Err(err) => self.notify(Err(err)),
        }

        Ok(())
    }

    pub fn uart_stop_slave_node_register(
        &self,
        message: UartMessage,
        mqtt_msg_sender: &mpsc::Sender<MqttMessage>,
    ) -> Result<()> {
        self.notify_state(Ok(()), IdentifyAreaState::Canceled);
        uart_response_handler::<ConfirmResponse, ()>(message, mqtt_msg_sender)
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
        match work_status.work_status_type {
            WorkStatusType::Search => {
                tracing::debug!("report search meter finished");
                self.notify_state(Ok(()), IdentifyAreaState::Finished);
            }
            WorkStatusType::IdentifyArea => {
                tracing::debug!("report identify area finished");
            }
            _ => {}
        }
        Ok(())
    }

    pub fn uart_slave_node_info_and_device_report(
        &self,
        message: UartMessage,
        uart_msg_sender: &mpsc::Sender<UartMessage>,
    ) -> Result<()> {
        // 回复
        let frame = Frame::new_response(message.frame.get_seq(), None, ConfirmResponse::default());
        let response = UartMessage::new(ReqInfo::new(&message.frame, None), frame);
        uart_msg_sender.send(response).unwrap();

        let report = ReportNodeInfoAndDeviceType::try_from(message.frame.into_app_data())?;

        let mut info = self.info.lock().unwrap();
        info.report_info.push(report);
        self.cond.notify_one();

        Ok(())
    }
}
