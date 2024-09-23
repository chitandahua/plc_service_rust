use super::node_config::{self, NodeInfo};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::path::PathBuf;
use std::sync::{mpsc, Condvar, Mutex, MutexGuard};
use std::time::Duration;
use tracing::{debug, error, info};

use crate::mqtt_handler::MqttTopicType;
use crate::mqtt_message::{MqttMessage, PayloadBody, Status};
use crate::mqtt_topic::MqttTopic;
use crate::protocol::app_data::{
    self, AddNodeRequest, Address, ConfirmResponse, DelNodeRequest, InitOperation, InitRequest,
    QueryNodeInfoRequest, QueryNodeInfoResponse, QueryNodeNumberRequest, QueryNodeNumberResponse,
};
use crate::protocol::Frame;
use crate::request_info::MqttReqInfo;
use crate::service::parse_response::uart_response_mqtt_handler;
use crate::service::{IntoMqttMessage, UartResponse};
use crate::{MqttMsgHandler, MqttResponseError, ReqInfo, Result, UartMessage, APP_NAME};

struct NodeConfig {
    node_config: node_config::NodeConfig,
    operation_result: Option<Result<()>>,
}

pub struct NodeManage {
    timeout: u64,
    node_conf: Mutex<NodeConfig>,
    cond: Condvar,
}

pub struct NodeInfoData {
    node_infos: Vec<NodeInfo>,
}

trait AcqFilesOperation {
    fn parse_node_infos(
        node_config: &node_config::NodeConfig,
        message: &MqttMessage,
        app: &str,
    ) -> Result<Vec<NodeInfo>>;

    fn operate_node_infos(
        node_config: &mut node_config::NodeConfig,
        app: &str,
        node_infos: &[NodeInfo],
        is_init: bool,
    ) -> Result<Vec<NodeInfo>>;

    fn create_uart_request(node_infos: Vec<NodeInfo>) -> impl Into<app_data::AppData>;

    fn update_node_config(
        node_config: &mut node_config::NodeConfig,
        app: &str,
        node_infos: &[NodeInfo],
    ) -> Result<()>;
}

struct AddAcqFiles;
struct DelAcqFiles;
struct ClearAcqFiles;

impl AcqFilesOperation for AddAcqFiles {
    fn parse_node_infos(
        _node_config: &node_config::NodeConfig,
        message: &MqttMessage,
        _app: &str,
    ) -> Result<Vec<NodeInfo>> {
        serde_json::from_str::<Value>(message.payload())
            .map_err(anyhow::Error::from)
            .and_then(|v| {
                v.get("body")
                    .ok_or_else(|| anyhow::anyhow!("body not exist"))
                    .map(|body_value| {
                        serde_json::from_value::<Vec<NodeInfo>>(body_value.clone())
                            .map_err(anyhow::Error::from)
                    })
            })?
    }

    fn operate_node_infos(
        node_config: &mut node_config::NodeConfig,
        app: &str,
        node_infos: &[NodeInfo],
        is_init: bool,
    ) -> Result<Vec<NodeInfo>> {
        match is_init {
            true => Ok(node_infos.to_vec()),
            false => node_infos.iter().try_fold(
                Vec::<NodeInfo>::new(),
                |mut uart_node_infos, node_info| {
                    if !node_config.add_node_info_exist(app, node_info, is_init)? {
                        uart_node_infos.push(node_info.clone())
                    }
                    Ok(uart_node_infos)
                },
            ),
        }
    }

    fn create_uart_request(node_infos: Vec<NodeInfo>) -> impl Into<app_data::AppData> {
        AddNodeRequest::new(
            node_infos
                .into_iter()
                .map(|n| n.to_uart_node_info())
                .collect(),
        )
    }

    fn update_node_config(
        node_config: &mut node_config::NodeConfig,
        app: &str,
        node_infos: &[NodeInfo],
    ) -> Result<()> {
        node_config.add_node_infos_checked(app, node_infos)
    }
}

impl AcqFilesOperation for DelAcqFiles {
    fn parse_node_infos(
        _node_config: &node_config::NodeConfig,
        message: &MqttMessage,
        _app: &str,
    ) -> Result<Vec<NodeInfo>> {
        // 与 AddAcqFiles 相同的实现
        AddAcqFiles::parse_node_infos(_node_config, message, _app)
    }

    fn operate_node_infos(
        node_config: &mut node_config::NodeConfig,
        app: &str,
        node_infos: &[NodeInfo],
        _is_init: bool,
    ) -> Result<Vec<NodeInfo>> {
        node_infos
            .iter()
            .try_fold(Vec::<NodeInfo>::new(), |mut uart_node_infos, node_info| {
                if node_config.should_remove_node_info(app, node_info)? {
                    uart_node_infos.push(node_info.clone());
                }
                Ok(uart_node_infos)
            })
    }

    fn create_uart_request(node_infos: Vec<NodeInfo>) -> impl Into<app_data::AppData> {
        DelNodeRequest::new(
            node_infos
                .into_iter()
                .map(|n| Address::from(n.acq_addr()))
                .collect(),
        )
    }

    fn update_node_config(
        node_config: &mut node_config::NodeConfig,
        app: &str,
        node_infos: &[NodeInfo],
    ) -> Result<()> {
        node_config.remove_node_infos_checked(app, node_infos)
    }
}

impl AcqFilesOperation for ClearAcqFiles {
    fn parse_node_infos(
        node_config: &node_config::NodeConfig,
        _message: &MqttMessage,
        app: &str,
    ) -> Result<Vec<NodeInfo>> {
        Ok(node_config
            .get_all_node_infos(app)
            .into_iter()
            .map(|n| (*n).clone())
            .collect())
    }

    fn operate_node_infos(
        node_config: &mut node_config::NodeConfig,
        app: &str,
        node_infos: &[NodeInfo],
        is_init: bool,
    ) -> Result<Vec<NodeInfo>> {
        DelAcqFiles::operate_node_infos(node_config, app, node_infos, is_init)
    }

    fn create_uart_request(node_infos: Vec<NodeInfo>) -> impl Into<app_data::AppData> {
        DelAcqFiles::create_uart_request(node_infos)
    }

    fn update_node_config(
        node_config: &mut node_config::NodeConfig,
        app: &str,
        node_infos: &[NodeInfo],
    ) -> Result<()> {
        DelAcqFiles::update_node_config(node_config, app, node_infos)
    }
}

const ACQ_FILES_CHUNK_SIZE: usize = 10;
impl NodeManage {
    pub fn new(config_path: Option<PathBuf>, timeout: u64) -> Result<Self> {
        Ok(Self {
            timeout,
            node_conf: Mutex::new(NodeConfig {
                node_config: node_config::NodeConfig::new(config_path)?,
                operation_result: None,
            }),
            cond: Condvar::new(),
        })
    }

    pub fn init(&self, mqtt_msg_handler: &mut MqttMsgHandler) {
        use crate::config::SCHEMA_PATH;
        use crate::schema_check;
        const ACQ_FILES_OBJECT: &str = "/acqFiles";
        let set_acq_files_topic = format!("{}{}{}", "+/set/request/", APP_NAME, ACQ_FILES_OBJECT);
        let get_acq_files_topic = format!("{}{}{}", "+/get/request/", APP_NAME, ACQ_FILES_OBJECT);
        let get_acq_files_num_topic = format!("{}{}{}", "+/get/request/", APP_NAME, "/acqFilesNum");
        let del_acq_files_topic = format!("{}{}{}", "+/set/request/", APP_NAME, "/delAcqFiles");
        let clear_acq_files_topic = format!("{}{}{}", "+/set/request/", APP_NAME, "/clearAcqFiles");

        mqtt_msg_handler.add_topic_filter(
            set_acq_files_topic,
            MqttTopicType::AddAcqFiles,
            schema_check::parse_schema(SCHEMA_PATH.join("add_node_schema.json")).ok(),
        );

        mqtt_msg_handler.add_topic_filter(
            get_acq_files_topic,
            MqttTopicType::GetAcqFiles,
            schema_check::parse_schema(SCHEMA_PATH.join("query_node_schema.json")).ok(),
        );

        mqtt_msg_handler.add_topic_filter(
            get_acq_files_num_topic,
            MqttTopicType::GetAcqFilesNum,
            schema_check::parse_schema(SCHEMA_PATH.join("query_node_num_schema.json")).ok(),
        );

        mqtt_msg_handler.add_topic_filter(
            del_acq_files_topic,
            MqttTopicType::DelAcqFiles,
            schema_check::parse_schema(SCHEMA_PATH.join("del_node_schema.json")).ok(),
        );

        mqtt_msg_handler.add_topic_filter(
            clear_acq_files_topic,
            MqttTopicType::ClearAcqFiles,
            schema_check::parse_schema(SCHEMA_PATH.join("clear_node_schema.json")).ok(),
        );
    }

    fn wait_operation_result(&self, mut node_conf: MutexGuard<'_, NodeConfig>) -> Result<()> {
        node_conf.operation_result = None; // 可去掉
        let result = self
            .cond
            .wait_timeout_while(node_conf, Duration::from_millis(self.timeout), |conf| {
                conf.operation_result.is_none()
            })
            .unwrap();
        node_conf = result.0;
        if result.1.timed_out() {
            node_conf.operation_result = Some(Err(anyhow::anyhow!(MqttResponseError::Timeout)));
        }

        node_conf.operation_result.take().unwrap()
    }
}

impl NodeManage {
    fn mqtt_opration_acq_files<T: AcqFilesOperation>(
        &self,
        message: MqttMessage,
        mqtt_msg_sender: &mpsc::Sender<MqttMessage>,
        uart_msg_sender: &mpsc::Sender<UartMessage>,
    ) -> Result<()> {
        let app = MqttTopic::get_app(message.topic());

        let node_config = self.node_conf.lock().unwrap();
        let node_infos = match T::parse_node_infos(&node_config.node_config, &message, app) {
            Err(e) => {
                mqtt_msg_sender.send(MqttMessage::new_with_msg_status_reason(
                    message,
                    Status::Failure,
                    e.to_string(),
                ))?;
                return Err(e);
            }
            Ok(node_infos) => node_infos,
        };
        drop(node_config); // 释放锁

        let result = self.operate_acq_files::<T>(
            app,
            Some(message.to_mqtt_req_info()),
            node_infos,
            uart_msg_sender,
        );
        let response = match result {
            Ok(_) => MqttMessage::new_with_msg_body(message, None),
            Err(e) => MqttMessage::new_with_msg_status_reason(message, Status::Failure, e),
        };
        mqtt_msg_sender.send(response)?;

        Ok(())
    }

    fn operate_chunk_acq_files<T: AcqFilesOperation>(
        &self,
        app: &str,
        mut mqtt_req_info: Option<MqttReqInfo>,
        node_infos: &[NodeInfo],
        uart_msg_sender: &mpsc::Sender<UartMessage>,
    ) -> Result<usize> {
        let mut node_config = self.node_conf.lock().unwrap();
        let operate_nodes = T::operate_node_infos(
            &mut node_config.node_config,
            app,
            node_infos,
            mqtt_req_info.is_none(),
        )?;

        // 无需uart操作
        let node_number = operate_nodes.len();
        if node_number == 0 {
            debug!("no need to operate uart");
            return Ok(0);
        }

        debug!("uart operate nodes: {:?}", operate_nodes);
        if let Some(mqtt_req_info) = mqtt_req_info.as_mut() {
            let extra_data = NodeInfoData {
                node_infos: operate_nodes.clone(),
            };
            mqtt_req_info.set_extra_data(Some(Box::new(extra_data)));
        }
        let frame = Frame::new_request(None, T::create_uart_request(operate_nodes));

        let req_info = ReqInfo::new(&frame, mqtt_req_info);
        uart_msg_sender.send(UartMessage::new(req_info, frame))?;

        self.wait_operation_result(node_config)
            .map(|_| node_number)?;

        Ok(node_number)
    }

    fn operate_acq_files<T: AcqFilesOperation>(
        &self,
        app: &str,
        mqtt_req_info: Option<MqttReqInfo>,
        node_infos: Vec<NodeInfo>,
        uart_msg_sender: &mpsc::Sender<UartMessage>,
    ) -> Result<()> {
        for (index, nodes) in node_infos.chunks(ACQ_FILES_CHUNK_SIZE).enumerate() {
            let mqtt_req_info = mqtt_req_info.as_ref().map(|mqtt_req_info| {
                MqttReqInfo::new(mqtt_req_info.topic(), mqtt_req_info.token(), None)
            });
            match self.operate_chunk_acq_files::<T>(app, mqtt_req_info, nodes, uart_msg_sender) {
                Ok(0) => {
                    info!(
                        "node index[{}-{}) no need to operate uart",
                        index,
                        index + nodes.len()
                    );
                }
                Ok(_) => {}
                Err(e) => {
                    error!("operate chunk acq files error: {}", e);
                    return Err(e);
                }
            }
        }

        Ok(())
    }

    pub fn mqtt_add_acq_files(
        &self,
        message: MqttMessage,
        mqtt_msg_sender: &mpsc::Sender<MqttMessage>,
        uart_msg_sender: &mpsc::Sender<UartMessage>,
    ) -> Result<()> {
        self.mqtt_opration_acq_files::<AddAcqFiles>(message, mqtt_msg_sender, uart_msg_sender)
    }

    pub fn mqtt_del_acq_files(
        &self,
        message: MqttMessage,
        mqtt_msg_sender: &mpsc::Sender<MqttMessage>,
        uart_msg_sender: &mpsc::Sender<UartMessage>,
    ) -> Result<()> {
        self.mqtt_opration_acq_files::<DelAcqFiles>(message, mqtt_msg_sender, uart_msg_sender)
    }

    pub fn mqtt_clear_acq_files(
        &self,
        message: MqttMessage,
        mqtt_msg_sender: &mpsc::Sender<MqttMessage>,
        uart_msg_sender: &mpsc::Sender<UartMessage>,
    ) -> Result<()> {
        self.mqtt_opration_acq_files::<ClearAcqFiles>(message, mqtt_msg_sender, uart_msg_sender)
    }

    fn uart_operate_acq_files<T: AcqFilesOperation>(&self, message: UartMessage) -> Result<()> {
        let response = UartResponse::<ConfirmResponse>::try_from(message.frame)?;
        let is_init = message.req_info.is_init();
        let result = match response {
            UartResponse::Normal(_) => Ok(()),
            UartResponse::Deny(response) => Err(anyhow::anyhow!("{}", response.error_code())),
        };

        if is_init || result.is_err() {
            let mut node_config = self.node_conf.lock().unwrap();
            node_config.operation_result = Some(result);
            self.cond.notify_one();
        } else {
            let mut mqtt_req_info = message.req_info.into_mqtt_req_info().unwrap();
            let extra_data = mqtt_req_info.get_extra_data().unwrap();
            let node_infos = extra_data.downcast::<NodeInfoData>().unwrap();
            let mut node_conf = self.node_conf.lock().unwrap();
            let result = T::update_node_config(
                &mut node_conf.node_config,
                MqttTopic::get_app(mqtt_req_info.topic()),
                &node_infos.node_infos,
            );
            node_conf.operation_result = Some(result);
            self.cond.notify_one();
        }

        Ok(())
    }

    pub fn uart_add_acq_files(&self, message: UartMessage) -> Result<()> {
        self.uart_operate_acq_files::<AddAcqFiles>(message)
    }

    pub fn uart_del_acq_files(&self, message: UartMessage) -> Result<()> {
        self.uart_operate_acq_files::<DelAcqFiles>(message)
    }

    // 仅初始化时调用
    pub fn init_clear_acq_files(&self, uart_msg_sender: &mpsc::Sender<UartMessage>) -> Result<()> {
        let frame = Frame::new_request(None, InitRequest::new(InitOperation::Params));
        let req_info = ReqInfo::new(&frame, None);

        let node_config = self.node_conf.lock().unwrap();
        uart_msg_sender.send(UartMessage::new(req_info, frame))?;
        self.wait_operation_result(node_config)
    }

    pub fn uart_clear_acq_files(&self, message: UartMessage) -> Result<()> {
        {
            let mut node_config = self.node_conf.lock().unwrap();
            node_config.node_config.clear_all_app();
        }
        self.uart_operate_acq_files::<ClearAcqFiles>(message)
    }

    pub fn load_config(&self, uart_msg_sender: &mpsc::Sender<UartMessage>) -> Result<()> {
        let nodes = {
            let mut node_config = self.node_conf.lock().unwrap();
            node_config.node_config.load_config()?
        };

        let mut uart_node_infos = Vec::new();
        for (_, node_infos) in nodes {
            // 去重
            for node_info in node_infos {
                if !uart_node_infos.contains(&node_info) {
                    uart_node_infos.push(node_info);
                }
            }
        }
        // 仅下发到uart 内存中配置在load_config中已加载
        self.operate_acq_files::<AddAcqFiles>("", None, uart_node_infos, uart_msg_sender)?;

        Ok(())
    }
}

#[derive(Debug, Deserialize)]
struct NodeInfoRequest {
    #[serde(rename = "startIndex")]
    start_index: String,
    #[serde(rename = "curMeterNum")]
    cur_meter_num: String,
    query_cco: Option<u8>,
}

#[derive(Debug, Deserialize)]
struct NodeNumberRequest {
    query_cco: Option<u8>,
}

#[derive(Debug, Serialize)]
pub struct NodeNumerResponse {
    #[serde(rename = "acqNum")]
    acq_num: String,
}

impl From<QueryNodeNumberResponse> for NodeNumerResponse {
    fn from(response: QueryNodeNumberResponse) -> Self {
        Self {
            acq_num: response.node_number.to_string(),
        }
    }
}

impl NodeManage {
    pub fn mqtt_get_acq_files(
        &self,
        message: MqttMessage,
        mqtt_msg_sender: &mpsc::Sender<MqttMessage>,
        uart_msg_sender: &mpsc::Sender<UartMessage>,
    ) -> Result<()> {
        let request = serde_json::from_str::<NodeInfoRequest>(message.payload())?;
        let start_index = request.start_index.parse::<usize>()?;
        let cur_meter_num = request.cur_meter_num.parse::<usize>()?;
        let app = MqttTopic::get_app(message.topic());

        if request
            .query_cco
            .map(|query_cco| query_cco != 0)
            .unwrap_or(false)
        {
            let frame = Frame::new_request(
                None,
                QueryNodeInfoRequest::new(start_index as u16, cur_meter_num as u8),
            );
            let req_info = ReqInfo::new(&frame, Some(message.to_mqtt_req_info()));

            uart_msg_sender.send(UartMessage::new(req_info, frame))?;
        } else {
            let body = {
                let node_config = self.node_conf.lock().unwrap();
                let node_infos =
                    node_config
                        .node_config
                        .get_node_infos(app, start_index, cur_meter_num);
                serde_json::to_value(node_infos)?
            };
            mqtt_msg_sender.send(MqttMessage::new_with_msg_body(
                message,
                Some(PayloadBody::Nested { body }),
            ))?;
        }

        Ok(())
    }

    pub fn uart_get_acq_files(
        &self,
        message: UartMessage,
        mqtt_msg_sender: &mpsc::Sender<MqttMessage>,
    ) -> Result<()> {
        uart_response_mqtt_handler::<QueryNodeInfoResponse>(message, mqtt_msg_sender)
    }

    pub fn mqtt_get_acq_files_number(
        &self,
        message: MqttMessage,
        mqtt_msg_sender: &mpsc::Sender<MqttMessage>,
        uart_msg_sender: &mpsc::Sender<UartMessage>,
    ) -> Result<()> {
        let request = serde_json::from_str::<NodeNumberRequest>(message.payload())?;
        let app = MqttTopic::get_app(message.topic());

        if request
            .query_cco
            .map(|query_cco| query_cco != 0)
            .unwrap_or(false)
        {
            let frame = Frame::new_request(None, QueryNodeNumberRequest);
            let req_info = ReqInfo::new(&frame, Some(message.to_mqtt_req_info()));

            uart_msg_sender.send(UartMessage::new(req_info, frame))?;
        } else {
            let node_number = {
                let node_config = self.node_conf.lock().unwrap();
                node_config.node_config.get_node_count(app)
            };
            let body = serde_json::to_value(NodeNumerResponse {
                acq_num: node_number.to_string(),
            })?;
            mqtt_msg_sender.send(MqttMessage::new_with_msg_body(
                message,
                Some(PayloadBody::Flat(body)),
            ))?;
        }

        Ok(())
    }

    pub fn uart_get_acq_files_number(
        &self,
        message: UartMessage,
        mqtt_msg_sender: &mpsc::Sender<MqttMessage>,
    ) -> Result<()> {
        uart_response_mqtt_handler::<QueryNodeNumberResponse>(message, mqtt_msg_sender)
    }
}

impl IntoMqttMessage for QueryNodeNumberResponse {
    fn into_mqtt_message(self, mqtt_req_info: MqttReqInfo) -> MqttMessage {
        MqttMessage::new_with_req_info_body(
            mqtt_req_info,
            Some(PayloadBody::Flat(
                serde_json::to_value(NodeNumerResponse::from(self)).unwrap(),
            )),
        )
    }
}

impl IntoMqttMessage for QueryNodeInfoResponse {
    fn into_mqtt_message(self, mqtt_req_info: MqttReqInfo) -> MqttMessage {
        let node_infos: Vec<NodeInfo> = self
            .into_node_infos()
            .into_iter()
            .map(|n| n.into())
            .collect();
        MqttMessage::new_with_req_info_body(
            mqtt_req_info,
            Some(PayloadBody::Nested {
                body: serde_json::to_value(&node_infos).unwrap(),
            }),
        )
    }
}
