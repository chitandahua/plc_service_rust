use crate::mqtt_handler::MqttTopicType;
use crate::mqtt_message::{PayloadBody, Status};
use crate::protocol::{app_data, Address, AddressField, Frame};
use crate::service::{parse_response::UartResponse, MqttReqInfo};
use crate::{MqttMessage, MqttMsgHandler, ReqInfo, Result, UartMessage, APP_NAME};

use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, Condvar, Mutex};

use crate::service::{ConcurrentMeter, RouteCtrl};

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
    _frame_timeout: u32,
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

trait GetAcqAddr {
    fn get_acq_addr(&self) -> Address;
}

impl GetAcqAddr for MonitorNodeDelayRequest {
    fn get_acq_addr(&self) -> Address {
        Address::from(self.acq_addr.as_str())
    }
}

impl GetAcqAddr for MonitorNodeDataRequest {
    fn get_acq_addr(&self) -> Address {
        Address::from(self.acq_addr.as_str())
    }
}

#[derive(Debug, Serialize)]
pub(crate) struct MonitorNodeDelayResponse {
    pub delay: u16,
}

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
            _ => unreachable!(),
        }
    }

    fn wait_data(&self) -> Result<MonitorNodeDataResponse> {
        match self.wait_for_response()? {
            MonitorNodeResponse::Data(response) => Ok(response),
            _ => unreachable!(),
        }
    }

    fn get_monitor_node_delay(
        &self,
        master_address: Address,
        route_ctrl: RouteCtrl,
        message: MqttMessage,
        uart_msg_sender: &mpsc::Sender<UartMessage>,
    ) -> Result<MonitorNodeDelayResponse> {
        route_ctrl.auto_pause_metering(uart_msg_sender)?;
        self.metering.store(true, Ordering::Relaxed);

        monitor_node_request::<MonitorNodeDelayRequest>(master_address, message, uart_msg_sender);

        // 等待delay以及回复
        self.wait_delay()
            .and_then(|delay| self.wait_data().map(|_| delay))
    }

    fn get_monitor_node_data(
        &self,
        master_address: Address,
        route_ctrl: RouteCtrl,
        message: MqttMessage,
        uart_msg_sender: &mpsc::Sender<UartMessage>,
    ) -> Result<MonitorNodeDataResponse> {
        route_ctrl.auto_pause_metering(uart_msg_sender)?;
        self.metering.store(true, Ordering::Relaxed);

        monitor_node_request::<MonitorNodeDataRequest>(master_address, message, uart_msg_sender);

        // 等待回复
        self.wait_data()
    }

    pub fn mqtt_get_monitor_node_delay(
        &self,
        master_address: Address,
        route_ctrl: RouteCtrl,
        _concurrent_meter: ConcurrentMeter,
        message: MqttMessage,
        mqtt_msg_sender: &mpsc::Sender<MqttMessage>,
        uart_msg_sender: &mpsc::Sender<UartMessage>,
    ) -> Result<()> {
        let mqtt_req_info = message.to_mqtt_req_info();
        let response =
            match self.get_monitor_node_delay(master_address, route_ctrl, message, uart_msg_sender)
            {
                Ok(res) => MqttMessage::new_with_req_info_body(
                    mqtt_req_info,
                    Some(PayloadBody::Flat(serde_json::to_value(res).unwrap())),
                ),
                Err(e) => {
                    MqttMessage::new_with_req_info_status_reason(mqtt_req_info, Status::Failure, e)
                }
            };

        self.metering.store(false, Ordering::Relaxed);
        //concurrent_meter.handle_request(); // TODO

        mqtt_msg_sender.send(response).unwrap();

        Ok(())
    }

    pub fn mqtt_get_monitor_node_data(
        &self,
        master_address: Address,
        route_ctrl: RouteCtrl,
        _concurrent_meter: ConcurrentMeter,
        message: MqttMessage,
        mqtt_msg_sender: &mpsc::Sender<MqttMessage>,
        uart_msg_sender: &mpsc::Sender<UartMessage>,
    ) -> Result<()> {
        let mqtt_req_info = message.to_mqtt_req_info();
        let response = match self.get_monitor_node_data(
            master_address,
            route_ctrl,
            message,
            uart_msg_sender,
        ) {
            Ok(res) => MqttMessage::new_with_req_info_body(
                mqtt_req_info,
                Some(PayloadBody::Flat(serde_json::to_value(res).unwrap())),
            ),
            Err(e) => {
                MqttMessage::new_with_req_info_status_reason(mqtt_req_info, Status::Failure, e)
            }
        };

        self.metering.store(false, Ordering::Relaxed);
        //concurrent_meter.handle_request(); // TODO

        mqtt_msg_sender.send(response).unwrap();

        Ok(())
    }

    pub fn uart_get_monitor_node_data(&self, message: UartMessage) -> Result<()> {
        let result: Result<app_data::MonitorNodeResponse> =
            UartResponse::<app_data::MonitorNodeResponse>::try_from(message.frame)?.into();
        let result = result.map(MonitorNodeDataResponse::from);
        let mut res = self.response.lock().unwrap();
        *res = Some(result.map(MonitorNodeResponse::Data));
        self.cond.notify_one();

        Ok(())
    }
}

fn monitor_node_request<T>(
    master_address: Address,
    message: MqttMessage,
    uart_msg_sender: &mpsc::Sender<UartMessage>,
) where
    T: serde::de::DeserializeOwned + GetAcqAddr,
    T: Into<app_data::MonitorNodeRequest>,
{
    let request: T = serde_json::from_str(message.payload()).unwrap();
    let acq_addr = request.get_acq_addr();
    let request: app_data::MonitorNodeRequest = request.into();
    let mqtt_req_info = MqttReqInfo::new(message.topic(), message.get_token(), None);

    let frame = Frame::new_request(
        Some(AddressField::new(master_address, None, acq_addr)),
        request,
    );
    let req_info = ReqInfo::new(&frame, Some(mqtt_req_info));
    uart_msg_sender
        .send(UartMessage::new(req_info, frame))
        .unwrap();
}
