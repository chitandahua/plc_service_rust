use crate::mqtt_handler::MqttTopicType;
use crate::mqtt_message::PayloadBody;
use crate::protocol::app_data::{self, Afn, RouteDataRead};
use crate::protocol::{Address, AddressField, Frame};
use crate::request_info::FrameKey;
use crate::service::{parse_response::UartResponse, MqttReqInfo};
use crate::{
    impl_into_mqtt_message, MqttMessage, MqttMsgHandler, MqttResponseError, ReqInfo, Result,
    UartMessage, APP_NAME,
};

use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, Condvar, Mutex};
use std::time::Duration;

use crate::service::{IntoMqttMessage, RouteCtrl};

#[derive(Clone)]
pub struct MonitorNode {
    metering: Arc<AtomicBool>,
    response: Arc<Mutex<Option<Result<MonitorNodeResponse>>>>,
    cond: Arc<Condvar>,
}

#[derive(Debug, Deserialize)]
struct MonitorNodeDelayRequest {
    #[serde(rename = "acqAddr")]
    acq_addr: String,
    #[serde(rename = "proType")]
    pro_type: u8,
    data: String,
}

impl From<MonitorNodeDelayRequest> for app_data::MonitorNodeRequest {
    fn from(req: MonitorNodeDelayRequest) -> Self {
        app_data::MonitorNodeRequest {
            protocol_type: req.pro_type,
            comm_delay_flag: 1,
            node_addrs: vec![Address::from(req.acq_addr.as_str())],
            message: hex::decode(req.data).unwrap(),
        }
    }
}

#[derive(Debug, Deserialize)]
struct MonitorNodeDataRequest {
    #[serde(rename = "acqAddr")]
    acq_addr: String,
    #[serde(rename = "proType")]
    pro_type: u8,
    #[serde(rename = "frameTimeout")]
    frame_timeout: u32,
    #[serde(rename = "charTimeout")]
    _char_timeout: u32,
    data: String,
}

impl From<MonitorNodeDataRequest> for app_data::MonitorNodeRequest {
    fn from(req: MonitorNodeDataRequest) -> Self {
        app_data::MonitorNodeRequest {
            protocol_type: req.pro_type,
            comm_delay_flag: 0,
            node_addrs: vec![Address::from(req.acq_addr.as_str())],
            message: hex::decode(req.data).unwrap(),
        }
    }
}

trait MonitorNodeOperation {
    fn into_uart_message(master_address: Address, message: MqttMessage) -> UartMessage;
    fn wait_response(monitor_node: &MonitorNode) -> impl IntoMqttMessage;
}

impl MonitorNodeOperation for MonitorNodeDelayRequest {
    fn into_uart_message(master_address: Address, message: MqttMessage) -> UartMessage {
        let request: Self = serde_json::from_str(message.payload()).unwrap();
        let mqtt_req_info = MqttReqInfo::new(message.topic(), message.get_token(), None);

        let frame = Frame::new_request(
            Some(AddressField::new(
                master_address,
                None,
                Address::from(request.acq_addr.as_str()),
            )),
            app_data::MonitorNodeRequest::from(request),
        );
        let req_info = ReqInfo::new(&frame, Some(mqtt_req_info));

        UartMessage::new_with_extra_req_info(
            req_info,
            frame,
            Some(ReqInfo::new_with_key_no_seq(
                FrameKey::new(Afn::RouteDataRead, RouteDataRead::CommDelay as u8),
                None,
            )),
        )
    }

    fn wait_response(monitor_node: &MonitorNode) -> impl IntoMqttMessage {
        monitor_node
            .wait_delay()
            .and_then(|delay| monitor_node.wait_data().map(|_| delay))
    }
}

impl MonitorNodeOperation for MonitorNodeDataRequest {
    fn into_uart_message(master_address: Address, message: MqttMessage) -> UartMessage {
        let request: Self = serde_json::from_str(message.payload()).unwrap();
        let mqtt_req_info = MqttReqInfo::new(message.topic(), message.get_token(), None);
        let timeout = Duration::from_secs(request.frame_timeout as u64);

        let frame = Frame::new_request(
            Some(AddressField::new(
                master_address,
                None,
                Address::from(request.acq_addr.as_str()),
            )),
            app_data::MonitorNodeRequest::from(request),
        );
        let req_info = ReqInfo::new(&frame, Some(mqtt_req_info));
        UartMessage::new_with_timeout(req_info, frame, timeout)
    }

    fn wait_response(monitor_node: &MonitorNode) -> impl IntoMqttMessage {
        monitor_node.wait_data()
    }
}

#[derive(Debug, Serialize)]
pub(crate) struct MonitorNodeDelayResponse {
    pub delay: u16,
}

impl_into_mqtt_message!(MonitorNodeDelayResponse, flat);

#[derive(Debug, Serialize)]
pub(crate) struct MonitorNodeDataResponse {
    pub data: String,
}

impl From<app_data::MonitorNodeResponse> for MonitorNodeDataResponse {
    fn from(response: app_data::MonitorNodeResponse) -> Self {
        MonitorNodeDataResponse {
            data: hex::encode(response.message),
        }
    }
}

impl_into_mqtt_message!(MonitorNodeDataResponse, flat);

#[derive(Debug)]
pub(crate) enum MonitorNodeResponse {
    Delay(MonitorNodeDelayResponse),
    Data(MonitorNodeDataResponse),
}

impl MonitorNode {
    pub fn new(metering: Arc<AtomicBool>) -> Self {
        Self {
            metering,
            response: Arc::new(Mutex::new(None)),
            cond: Arc::new(Condvar::new()),
        }
    }

    pub fn init(&self, mqtt_msg_handler: &mut MqttMsgHandler) {
        use crate::config::SCHEMA_PATH;
        use crate::schema_check;
        let topic = format!("{}{}{}", "+/get/request/", APP_NAME, "/monitorNode");
        let schema = schema_check::parse_schema(SCHEMA_PATH.join("monitor_node_schema.json")).ok();
        mqtt_msg_handler.add_topic_filter(topic, MqttTopicType::MonitorNode, schema);

        let topic = format!("{}{}{}", "+/get/request/", APP_NAME, "/monitorNodeDelay");
        let schema =
            schema_check::parse_schema(SCHEMA_PATH.join("monitor_node_delay_schema.json")).ok();
        mqtt_msg_handler.add_topic_filter(topic, MqttTopicType::MonitorNodeDelay, schema);
    }

    fn wait_for_response(&self) -> Result<MonitorNodeResponse> {
        let mut res = self.response.lock().unwrap();
        res = self.cond.wait_while(res, |r| r.is_none()).unwrap();

        res.take().unwrap()
    }

    fn wait_delay(&self) -> Result<MonitorNodeDelayResponse> {
        match self.wait_for_response()? {
            MonitorNodeResponse::Delay(response) => Ok(response),
            _ => Err(anyhow::anyhow!("Invalid monitor node response")),
        }
    }

    fn wait_data(&self) -> Result<MonitorNodeDataResponse> {
        match self.wait_for_response()? {
            MonitorNodeResponse::Data(response) => Ok(response),
            _ => Err(anyhow::anyhow!("Invalid monitor node response")),
        }
    }

    fn with_metering<F, T>(&self, f: F) -> T
    where
        F: FnOnce() -> T,
    {
        self.metering.store(true, Ordering::Relaxed);
        let result = f();
        self.metering.store(false, Ordering::Relaxed);
        result
    }

    fn get_monitor_node_info<R: MonitorNodeOperation>(
        &self,
        master_address: Address,
        route_ctrl: &RouteCtrl,
        message: MqttMessage,
        uart_msg_sender: &mpsc::Sender<UartMessage>,
    ) -> MqttMessage {
        let mqtt_req_info = message.to_mqtt_req_info();
        route_ctrl
            .auto_pause_metering(uart_msg_sender)
            .map(|_| {
                self.with_metering(move || {
                    let uart_message = R::into_uart_message(master_address, message);
                    uart_msg_sender.send(uart_message).unwrap();
                    R::wait_response(self)
                })
            })
            .into_mqtt_message(mqtt_req_info)
    }

    fn mqtt_get_monitor_node_info<R: MonitorNodeOperation>(
        &self,
        master_address: Address,
        route_ctrl: &RouteCtrl,
        message: MqttMessage,
        mqtt_msg_sender: &mpsc::Sender<MqttMessage>,
        uart_msg_sender: &mpsc::Sender<UartMessage>,
    ) -> Result<()> {
        let response =
            self.get_monitor_node_info::<R>(master_address, route_ctrl, message, uart_msg_sender);

        mqtt_msg_sender.send(response).unwrap();

        Ok(())
    }

    pub fn mqtt_get_monitor_node_delay(
        &self,
        master_address: Address,
        route_ctrl: &RouteCtrl,
        message: MqttMessage,
        mqtt_msg_sender: &mpsc::Sender<MqttMessage>,
        uart_msg_sender: &mpsc::Sender<UartMessage>,
    ) -> Result<()> {
        self.mqtt_get_monitor_node_info::<MonitorNodeDelayRequest>(
            master_address,
            route_ctrl,
            message,
            mqtt_msg_sender,
            uart_msg_sender,
        )
    }

    pub fn mqtt_get_monitor_node_data(
        &self,
        master_address: Address,
        route_ctrl: &RouteCtrl,
        message: MqttMessage,
        mqtt_msg_sender: &mpsc::Sender<MqttMessage>,
        uart_msg_sender: &mpsc::Sender<UartMessage>,
    ) -> Result<()> {
        self.mqtt_get_monitor_node_info::<MonitorNodeDataRequest>(
            master_address,
            route_ctrl,
            message,
            mqtt_msg_sender,
            uart_msg_sender,
        )
    }

    pub fn uart_notify_monitor_node_dalay(&self, delay: u16) {
        let mut res = self.response.lock().unwrap();
        *res = Some(Ok(MonitorNodeResponse::Delay(MonitorNodeDelayResponse {
            delay,
        })));
        self.cond.notify_one();
    }

    pub fn uart_get_monitor_node_data(
        &self,
        route_ctrl: &RouteCtrl,
        message: UartMessage,
        uart_msg_sender: &mpsc::Sender<UartMessage>,
    ) -> Result<()> {
        let result: Result<app_data::MonitorNodeResponse> =
            UartResponse::<app_data::MonitorNodeResponse>::try_from(message.frame)?.into();
        let result = result.map(MonitorNodeDataResponse::from);
        let mut res = self.response.lock().unwrap();
        *res = Some(result.map(MonitorNodeResponse::Data));
        self.cond.notify_one();

        route_ctrl.uart_response_update_resume_timer(uart_msg_sender);

        Ok(())
    }

    pub fn uart_monitor_node_timeout(&self) {
        let mut result = self.response.lock().unwrap();
        *result = Some(Err(anyhow::anyhow!(MqttResponseError::Timeout)));
        self.cond.notify_one();
    }
}
