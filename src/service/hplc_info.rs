use serde::{Deserialize, Serialize};
use std::any::Any;
use std::sync::{mpsc, Arc, Condvar, Mutex};

use crate::mqtt_handler::MqttTopicType;
use crate::mqtt_message::PayloadBody;
use crate::protocol::app_data::{
    module_id_format_string, ChipInfoRequest, ChipInfoResponse, IdInfoRequest, IdInfoResponse,
    MultipleNetRequest, MultipleNetResponse, NetTopologyRequest, NetTopologyResponse,
    QueryNodeLineInfoRequest, QueryNodeLineInfoResponse, SlaveModuleIdRequest,
    SlaveModuleIdResponse,
};
use crate::protocol::AppData;
use crate::request_info::{MqttReqInfo, UartMessage};
use crate::service::parse_response::{
    mqtt_info_request_uart_handler, mqtt_request_uart_handler, uart_response_mqtt_handler,
};
use crate::service::{IntoMqttMessage, UartResponse};
use crate::{MqttMessage, MqttMsgHandler, MqttResponseError, Result, APP_NAME};

const QUERY_NODE_NUMBER: u8 = 10;

trait HplcInfoResponse {
    fn total_number(&self) -> u16;
    fn item_number(&self) -> u16;
    fn extend(&mut self, item: Box<dyn Any + Send + Sync>);
}

// dyn HplcInfoResponse TODO 用不了downcast...  // :Any也不行
struct MqttHplcInfo {
    items: Option<Box<dyn Any + Send + Sync>>,
    result: Option<Result<Box<dyn Any + Send + Sync>>>,
}

#[derive(Clone)]
pub struct HplcInfo {
    info: Arc<Mutex<MqttHplcInfo>>,
    cond: Arc<Condvar>,
}

impl HplcInfo {
    pub fn new() -> Self {
        Self {
            info: Arc::new(Mutex::new(MqttHplcInfo {
                items: None,
                result: None,
            })),
            cond: Arc::new(Condvar::new()),
        }
    }
}

#[derive(Debug, Deserialize)]
struct HplcInfoRequest {
    #[serde(rename = "startIndex")]
    start_index: u16,
    #[serde(rename = "nodeNumber")]
    node_number: u8,
}

trait HplcInfoNew {
    fn new(start_index: u16, node_number: u8) -> Self;
}

impl HplcInfoNew for HplcInfoRequest {
    fn new(start_index: u16, node_number: u8) -> Self {
        Self {
            start_index,
            node_number,
        }
    }
}

impl HplcInfo {
    pub fn mqtt_get_chip_info(message: MqttMessage, uart_msg_sender: &mpsc::Sender<UartMessage>) {
        let req = serde_json::from_str::<HplcInfoRequest>(message.payload()).unwrap();
        mqtt_request_uart_handler::<ChipInfoRequest>(
            ChipInfoRequest::new(req.start_index, req.node_number),
            message,
            uart_msg_sender,
        );
    }

    pub fn chip_info_response(
        message: UartMessage,
        sender: &mpsc::Sender<MqttMessage>,
    ) -> Result<()> {
        uart_response_mqtt_handler::<ChipInfoResponse>(message, sender)
    }

    pub fn init(mqtt_msg_handler: &mut MqttMsgHandler) {
        use crate::config::SCHEMA_PATH;
        use crate::schema_check;
        let topic = format!("{}{}{}", "+/get/request/", APP_NAME, "/chipInformation");
        let schema = schema_check::parse_schema(SCHEMA_PATH.join("get_chip_info_schema.json")).ok();
        mqtt_msg_handler.add_topic_filter(topic, MqttTopicType::GetChipInfo, schema);

        let topic = format!("{}{}{}", "+/get/request/", APP_NAME, "/idInformation");
        let schema = schema_check::parse_schema(SCHEMA_PATH.join("get_id_info_schema.json")).ok();
        mqtt_msg_handler.add_topic_filter(topic, MqttTopicType::GetIdInfo, schema);

        let topic = format!("{}{}{}", "+/get/request/", APP_NAME, "/nodeLineInformation");
        let schema =
            schema_check::parse_schema(SCHEMA_PATH.join("get_node_line_info_schema.json")).ok();
        mqtt_msg_handler.add_topic_filter(topic, MqttTopicType::GetNodeLineInfo, schema);

        let topic = format!("{}{}{}", "+/get/request/", APP_NAME, "/slaveModeID");
        let schema =
            schema_check::parse_schema(SCHEMA_PATH.join("get_slave_module_id_schema.json")).ok();
        mqtt_msg_handler.add_topic_filter(topic, MqttTopicType::GetSlaveModuleId, schema);

        let topic = format!("{}{}{}", "+/get/request/", APP_NAME, "/netTopoInfo");
        let schema =
            schema_check::parse_schema(SCHEMA_PATH.join("get_net_topology_schema.json")).ok();
        mqtt_msg_handler.add_topic_filter(topic, MqttTopicType::GetNetTopology, schema);

        let topic = format!("{}{}{}", "+/get/request/", APP_NAME, "/multiNetInformation");
        let schema = schema_check::parse_schema(SCHEMA_PATH.join("get_multi_net_schema.json")).ok();
        mqtt_msg_handler.add_topic_filter(topic, MqttTopicType::GetMultiNet, schema);
    }
}

#[derive(Debug, Serialize)]
struct MqttChipInfo {
    #[serde(rename = "nodeSN")]
    node_sn: u16,
    #[serde(rename = "nodeAddr")]
    node_addr: String,
    #[serde(rename = "devType")]
    dev_type: u8,
    #[serde(rename = "chipID")]
    chip_id: String,
    #[serde(rename = "chipSoftVer")]
    chip_soft_ver: String,
}

#[derive(Debug, Serialize)]
struct MqttChipInfoResponse(Vec<MqttChipInfo>);

impl From<ChipInfoResponse> for MqttChipInfoResponse {
    fn from(chip_info_response: ChipInfoResponse) -> Self {
        Self(
            chip_info_response
                .chip_infos
                .into_iter()
                .enumerate()
                .map(|(index, chip_info)| MqttChipInfo {
                    node_sn: chip_info_response.start_seq + index as u16,
                    node_addr: chip_info.address.to_string(),
                    dev_type: chip_info.device_type,
                    chip_id: hex::encode(chip_info.id_info),
                    chip_soft_ver: chip_info.software_version,
                })
                .collect(),
        )
    }
}

impl IntoMqttMessage for ChipInfoResponse {
    fn into_mqtt_message(self, mqtt_req_info: MqttReqInfo) -> MqttMessage {
        MqttMessage::new_with_req_info_body(
            mqtt_req_info,
            Some(PayloadBody::Nested {
                body: serde_json::to_value(MqttChipInfoResponse::from(self)).unwrap(),
            }),
        )
    }
}

#[derive(Debug, Serialize)]
struct MqttNodeLineInfo {
    #[serde(rename = "nodeSN")]
    node_sn: u16,
    #[serde(rename = "nodeAddr")]
    node_addr: String,
    #[serde(rename = "nodeLineInfo")]
    node_line_info: String,
}

#[derive(Debug, Serialize)]
struct MqttNodeLineInfoResponse(Vec<MqttNodeLineInfo>);

impl From<QueryNodeLineInfoResponse> for MqttNodeLineInfoResponse {
    fn from(node_line_info_response: QueryNodeLineInfoResponse) -> Self {
        Self(
            node_line_info_response
                .line_infos
                .into_iter()
                .enumerate()
                .map(|(index, chip_info)| MqttNodeLineInfo {
                    node_sn: node_line_info_response.start_index + index as u16,
                    node_addr: chip_info.addr.to_string(),
                    node_line_info: format!("{:08b}", chip_info.info as u8),
                })
                .collect(),
        )
    }
}

impl IntoMqttMessage for QueryNodeLineInfoResponse {
    fn into_mqtt_message(self, mqtt_req_info: MqttReqInfo) -> MqttMessage {
        MqttMessage::new_with_req_info_body(
            mqtt_req_info,
            Some(PayloadBody::Nested {
                body: serde_json::to_value(MqttNodeLineInfoResponse::from(self)).unwrap(),
            }),
        )
    }
}

type NodeLineInfoRequest = HplcInfoRequest;

impl HplcInfo {
    pub fn mqtt_get_node_line_info(
        message: MqttMessage,
        uart_msg_sender: &mpsc::Sender<UartMessage>,
    ) {
        let req = serde_json::from_str::<NodeLineInfoRequest>(message.payload()).unwrap();
        mqtt_request_uart_handler::<QueryNodeLineInfoRequest>(
            QueryNodeLineInfoRequest::new(req.start_index, req.node_number),
            message,
            uart_msg_sender,
        );
    }

    pub fn uart_node_line_info_response(
        message: UartMessage,
        sender: &mpsc::Sender<MqttMessage>,
    ) -> Result<()> {
        uart_response_mqtt_handler::<QueryNodeLineInfoResponse>(message, sender)
    }
}

#[derive(Debug, Deserialize)]
struct MqttIdInfoRequest {
    #[serde(rename = "deviceType")]
    device_type: u8,
    #[serde(rename = "nodeAddr")]
    node_addr: String,
    #[serde(rename = "idType")]
    id_type: u8,
}

impl From<MqttIdInfoRequest> for IdInfoRequest {
    fn from(value: MqttIdInfoRequest) -> Self {
        Self {
            device_type: value.device_type,
            address: value.node_addr.as_str().into(),
            id_type: value.id_type,
        }
    }
}

#[derive(Debug, Serialize)]
struct MqttIdInfoResponse {
    #[serde(rename = "deviceType")]
    device_type: u8,
    #[serde(rename = "nodeAddr")]
    node_addr: String,
    #[serde(rename = "idType")]
    id_type: u8,
    #[serde(rename = "idLength")]
    id_length: u8,
    #[serde(rename = "idInformation")]
    id_info: String,
}

impl From<IdInfoResponse> for MqttIdInfoResponse {
    fn from(id_info_response: IdInfoResponse) -> Self {
        Self {
            device_type: id_info_response.device_type,
            node_addr: id_info_response.address.to_string(),
            id_type: id_info_response.id_type,
            id_length: id_info_response.id_length,
            id_info: hex::encode(id_info_response.id_info),
        }
    }
}

impl IntoMqttMessage for IdInfoResponse {
    fn into_mqtt_message(self, mqtt_req_info: MqttReqInfo) -> MqttMessage {
        MqttMessage::new_with_req_info_body(
            mqtt_req_info,
            Some(PayloadBody::Nested {
                body: serde_json::to_value(MqttIdInfoResponse::from(self)).unwrap(),
            }),
        )
    }
}

impl HplcInfo {
    pub fn mqtt_get_id_info(message: MqttMessage, uart_msg_sender: &mpsc::Sender<UartMessage>) {
        let req = serde_json::from_str::<MqttIdInfoRequest>(message.payload()).unwrap();
        mqtt_request_uart_handler::<IdInfoRequest>(
            IdInfoRequest::from(req),
            message,
            uart_msg_sender,
        );
    }

    pub fn uart_id_info_response(
        message: UartMessage,
        sender: &mpsc::Sender<MqttMessage>,
    ) -> Result<()> {
        uart_response_mqtt_handler::<IdInfoResponse>(message, sender)
    }
}

#[derive(Debug, Serialize)]
struct MqttSlaveModuleIdInfo {
    #[serde(rename = "nodeAddr")]
    node_addr: String,
    #[serde(rename = "devType")]
    dev_type: u8,
    #[serde(rename = "upgFlag")]
    upg_flag: u8,
    #[serde(rename = "vendorCode")]
    vendor_code: String,
    #[serde(rename = "modeIDLen")]
    mode_id_len: u8,
    #[serde(rename = "modeIDFormat")]
    mode_id_format: u8,
    #[serde(rename = "modeIDInfo")]
    mode_id_info: String,
}

#[derive(Debug, Serialize)]
struct MqttSlaveModuleIdInfoResponse(Vec<MqttSlaveModuleIdInfo>);

impl From<SlaveModuleIdResponse> for MqttSlaveModuleIdInfoResponse {
    fn from(slave_module_id_info_response: SlaveModuleIdResponse) -> Self {
        Self(
            slave_module_id_info_response
                .slave_module_id_infos
                .into_iter()
                .map(|slave_module_id_info| MqttSlaveModuleIdInfo {
                    node_addr: slave_module_id_info.address.to_string(),
                    dev_type: slave_module_id_info.device_type & 0x0f,
                    upg_flag: (slave_module_id_info.device_type & 0x80) >> 7,
                    vendor_code: slave_module_id_info.factory_code,
                    mode_id_len: slave_module_id_info.id_info.len() as u8,
                    mode_id_format: slave_module_id_info.id_format as u8,
                    mode_id_info: module_id_format_string(
                        slave_module_id_info.id_format,
                        &slave_module_id_info.id_info,
                    ),
                })
                .collect(),
        )
    }
}

impl IntoMqttMessage for SlaveModuleIdResponse {
    fn into_mqtt_message(self, mqtt_req_info: MqttReqInfo) -> MqttMessage {
        MqttMessage::new_with_req_info_body(
            mqtt_req_info,
            Some(PayloadBody::Nested {
                body: serde_json::to_value(MqttSlaveModuleIdInfoResponse::from(self)).unwrap(),
            }),
        )
    }
}

type MqttSlaveModuleIdRequest = HplcInfoRequest;

impl HplcInfo {
    pub fn mqtt_get_slave_module_id_info(
        message: MqttMessage,
        uart_msg_sender: &mpsc::Sender<UartMessage>,
    ) {
        let req = serde_json::from_str::<MqttSlaveModuleIdRequest>(message.payload()).unwrap();
        mqtt_request_uart_handler::<SlaveModuleIdRequest>(
            SlaveModuleIdRequest::new(req.start_index, req.node_number),
            message,
            uart_msg_sender,
        );
    }

    pub fn uart_slave_module_id_info_response(
        message: UartMessage,
        sender: &mpsc::Sender<MqttMessage>,
    ) -> Result<()> {
        uart_response_mqtt_handler::<SlaveModuleIdResponse>(message, sender)
    }
}

#[derive(Debug, Serialize)]
struct MqttNetTopologyInfo {
    #[serde(rename = "nodeAddr")]
    node_addr: String,
    #[serde(rename = "tei")]
    node_flag: u16,
    #[serde(rename = "proxyTei")]
    proxy_node_flag: u16,
    #[serde(rename = "nodeLevel")]
    node_level: u8,
    #[serde(rename = "nodeRole")]
    node_role: u8,
}

#[derive(Debug, Default, Serialize)]
struct MqttNetTopologyInfoResponse {
    #[serde(rename = "totalNumber")]
    total_number: u16,
    #[serde(rename = "body")]
    net_topology_infos: Vec<MqttNetTopologyInfo>,
}

impl From<NetTopologyResponse> for MqttNetTopologyInfoResponse {
    fn from(net_topology_response: NetTopologyResponse) -> Self {
        Self {
            total_number: net_topology_response.total_node_number,
            net_topology_infos: net_topology_response
                .net_topology_infos
                .into_iter()
                .map(|net_topology_info| MqttNetTopologyInfo {
                    node_addr: net_topology_info.address.to_string(),
                    node_flag: net_topology_info.node_flag,
                    proxy_node_flag: net_topology_info.proxy_node_flag,
                    node_level: net_topology_info.node_level,
                    node_role: net_topology_info.node_role,
                })
                .collect(),
        }
    }
}

impl HplcInfoResponse for MqttNetTopologyInfoResponse {
    fn item_number(&self) -> u16 {
        self.net_topology_infos.len() as u16
    }

    fn total_number(&self) -> u16 {
        self.total_number
    }

    fn extend(&mut self, item: Box<dyn Any + Send + Sync>) {
        let data = item.downcast::<Self>().unwrap();
        self.total_number = data.total_number;
        self.net_topology_infos.extend(data.net_topology_infos);
    }
}

impl IntoMqttMessage for MqttNetTopologyInfoResponse {
    fn into_mqtt_message(self, mqtt_req_info: MqttReqInfo) -> MqttMessage {
        MqttMessage::new_with_req_info_body(
            mqtt_req_info,
            Some(PayloadBody::Flat(serde_json::to_value(self).unwrap())),
        )
    }
}

impl IntoMqttMessage for NetTopologyResponse {
    fn into_mqtt_message(self, mqtt_req_info: MqttReqInfo) -> MqttMessage {
        MqttNetTopologyInfoResponse::from(self).into_mqtt_message(mqtt_req_info)
    }
}

impl From<HplcInfoRequest> for NetTopologyRequest {
    fn from(value: HplcInfoRequest) -> Self {
        Self::new(value.start_index, value.node_number)
    }
}

type MqttNetTopologyInfoRequest = HplcInfoRequest;

impl HplcInfo {
    pub fn _mqtt_get_net_topology_info(
        message: MqttMessage,
        uart_msg_sender: &mpsc::Sender<UartMessage>,
    ) {
        let req = serde_json::from_str::<MqttNetTopologyInfoRequest>(message.payload()).unwrap();
        mqtt_request_uart_handler::<NetTopologyRequest>(
            NetTopologyRequest::new(req.start_index, req.node_number),
            message,
            uart_msg_sender,
        );
    }

    pub fn _uart_net_topology_info_response(
        message: UartMessage,
        sender: &mpsc::Sender<MqttMessage>,
    ) -> Result<()> {
        uart_response_mqtt_handler::<NetTopologyResponse>(message, sender)
    }
}

impl HplcInfo {
    fn uart_hplc_info_response<T, R>(&self, message: UartMessage) -> Result<()>
    where
        T: TryFrom<AppData, Error = crate::Error>,
        R: HplcInfoResponse + From<T> + Send + Sync + 'static,
    {
        let response = UartResponse::<T>::try_from(message.frame)?;
        let mut info = self.info.lock().unwrap();
        let result: Result<T> = response.into();
        info.result =
            Some(result.map(|response| Box::new(R::from(response)) as Box<dyn Any + Send + Sync>));
        self.cond.notify_one();

        Ok(())
    }

    fn mqtt_hplc_info_request<M, T, R>(
        &self,
        message: MqttMessage,
        mqtt_msg_sender: &mpsc::Sender<MqttMessage>,
        uart_msg_sender: &mpsc::Sender<UartMessage>,
    ) where
        M: HplcInfoNew,
        T: Into<AppData> + From<M>,
        R: HplcInfoResponse + Default + IntoMqttMessage + Send + Sync + 'static,
    {
        let mqtt_req_info = message.to_mqtt_req_info();
        let mut info = self.info.lock().unwrap();
        info.items = Some(Box::new(R::default()));
        let response = loop {
            let items = info.items.as_mut().unwrap().downcast_mut::<R>().unwrap();
            let start_index = 1 + items.item_number();
            mqtt_info_request_uart_handler::<T>(
                T::from(M::new(start_index, QUERY_NODE_NUMBER)),
                Some(MqttReqInfo::default()),
                uart_msg_sender,
            );
            info = self
                .cond
                .wait_while(info, |info| info.result.is_none())
                .unwrap();
            let data = match info.result.take().unwrap() {
                Err(e) => {
                    break e.into_mqtt_message(mqtt_req_info);
                }
                Ok(data) => data.downcast::<R>().unwrap(),
            };
            let number = data.item_number();
            let items = info.items.as_mut().unwrap().downcast_mut::<R>().unwrap();
            items.extend(data);

            tracing::debug!(
                "item number: {}, total number: {}",
                items.item_number(),
                items.total_number()
            );
            if number == 0 || items.item_number() >= items.total_number() {
                break info
                    .items
                    .take()
                    .unwrap()
                    .downcast::<R>()
                    .unwrap()
                    .into_mqtt_message(mqtt_req_info);
            }
        };

        mqtt_msg_sender.send(response).unwrap();
    }

    pub fn mqtt_net_topology_info(
        &self,
        message: MqttMessage,
        mqtt_msg_sender: &mpsc::Sender<MqttMessage>,
        uart_msg_sender: &mpsc::Sender<UartMessage>,
    ) {
        self.mqtt_hplc_info_request::<MqttNetTopologyInfoRequest, NetTopologyRequest, MqttNetTopologyInfoResponse>(
            message,
            mqtt_msg_sender,
            uart_msg_sender,
        );
    }

    pub fn uart_net_topology_response(&self, message: UartMessage) -> Result<()> {
        self.uart_hplc_info_response::<NetTopologyResponse, MqttNetTopologyInfoResponse>(message)
    }

    pub fn uart_net_topology_response_timeout(&self) {
        let mut info = self.info.lock().unwrap();
        info.result = Some(Err(anyhow::anyhow!(MqttResponseError::Timeout)));
        self.cond.notify_one();
    }
}

#[derive(Debug, Serialize)]
struct MqttMultipleNetInfo {
    #[serde(rename = "nearNodeNID")]
    net_identity: String,
}

#[derive(Debug, Serialize)]
struct MqttMultipleNetInfoResponse {
    #[serde(rename = "selfNID")]
    node_net_identity: String,
    #[serde(rename = "selfMasterAddr")]
    address: String,
    body: Vec<MqttMultipleNetInfo>,
}

impl From<MultipleNetResponse> for MqttMultipleNetInfoResponse {
    fn from(multiple_net_response: MultipleNetResponse) -> Self {
        Self {
            node_net_identity: multiple_net_response.node_net_identity.to_string(),
            address: multiple_net_response.address.to_string(),
            body: multiple_net_response
                .multiple_net_infos
                .into_iter()
                .map(|multiple_net_info| MqttMultipleNetInfo {
                    net_identity: multiple_net_info.net_identity.to_string(),
                })
                .collect(),
        }
    }
}

impl IntoMqttMessage for MultipleNetResponse {
    fn into_mqtt_message(self, mqtt_req_info: MqttReqInfo) -> MqttMessage {
        MqttMessage::new_with_req_info_body(
            mqtt_req_info,
            Some(PayloadBody::Flat(
                serde_json::to_value(MqttMultipleNetInfoResponse::from(self)).unwrap(),
            )),
        )
    }
}

impl HplcInfo {
    pub fn mqtt_get_multiple_net_info(
        message: MqttMessage,
        uart_msg_sender: &mpsc::Sender<UartMessage>,
    ) {
        mqtt_request_uart_handler::<MultipleNetRequest>(
            MultipleNetRequest,
            message,
            uart_msg_sender,
        );
    }

    pub fn uart_multiple_net_info_response(
        message: UartMessage,
        sender: &mpsc::Sender<MqttMessage>,
    ) -> Result<()> {
        uart_response_mqtt_handler::<MultipleNetResponse>(message, sender)
    }
}
