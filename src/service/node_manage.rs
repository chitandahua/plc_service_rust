use super::node_config::{self, NodeInfo};
use serde_json::Value;
use std::path::PathBuf;
use std::sync::{mpsc, Arc, Condvar, Mutex, MutexGuard};
use std::time::Duration;
use tracing::{debug, error, info};

use crate::mqtt_handler::MqttTopicType;
use crate::mqtt_message::MqttMessage;
use crate::mqtt_topic::MqttTopic;
use crate::protocol::app_data::{
    self, AddNodeRequest, Address, ConfirmResponse, DelNodeRequest, InitOperation,
};
use crate::protocol::Frame;
use crate::request_info::MqttReqInfo;
use crate::service::UartResponse;
use crate::UartMessage;
use crate::{MqttMsgHandler, ReqInfo, Result, APP_NAME};

struct NodeConfig {
    node_config: node_config::NodeConfig,
    operation_result: Option<Result<()>>,
}

pub struct NodeManage {
    config_path: Option<PathBuf>,
    timeout: u64,
    node_conf: Mutex<NodeConfig>,
    cond: Condvar,
}

pub struct NodeInfoData {
    node_infos: Vec<NodeInfo>,
}

//pub struct NodeAddressData {
//    node_addresses: Vec<Address>,
//}

#[derive(Debug)]
#[repr(u8)]
enum AcqFileOperation {
    Add,
    Del,
    Clear,
}

const ACQ_FILES_CHUNK_SIZE: usize = 10;
impl NodeManage {
    pub fn new(config_path: Option<PathBuf>, timeout: u64) -> Self {
        Self {
            config_path,
            timeout,
            node_conf: Mutex::new(NodeConfig {
                node_config: node_config::NodeConfig::new(),
                operation_result: None,
            }),
            cond: Condvar::new(),
        }
    }

    pub fn init(&self, mqtt_msg_handler: &mut MqttMsgHandler) {
        const ACQ_FILES_OBJECT: &str = "/acqFiles";
        let set_acq_files_topic = format!("{}{}{}", "+/set/request/", APP_NAME, ACQ_FILES_OBJECT);
        let get_acq_files_topic = format!("{}{}{}", "+/get/request/", APP_NAME, ACQ_FILES_OBJECT);
        let get_acq_files_num_topic = format!("{}{}{}", "+/get/request/", APP_NAME, "/acqFilesNum");
        let del_acq_files_topic = format!("{}{}{}", "+/set/request/", APP_NAME, "/delAcqFiles");
        let clear_acq_files_topic = format!("{}{}{}", "+/set/request/", APP_NAME, "/clearAcqFiles");

        let topic_filters = vec![
            (set_acq_files_topic, MqttTopicType::AddAcqFiles),
            (get_acq_files_topic, MqttTopicType::GetAcqFiles),
            (get_acq_files_num_topic, MqttTopicType::GetAcqFilesNum),
            (del_acq_files_topic, MqttTopicType::DelAcqFiles),
            (clear_acq_files_topic, MqttTopicType::ClearAcqFiles),
        ];

        mqtt_msg_handler.add_topic_filters(topic_filters);
    }

    fn wait_operation_result(&self, mut node_conf: MutexGuard<'_, NodeConfig>) -> Result<()> {
        node_conf.operation_result = None; // 可去掉
        let result = self
            .cond
            .wait_timeout_while(node_conf, Duration::from_secs(self.timeout), |conf| {
                conf.operation_result.is_none()
            })
            .unwrap();
        node_conf = result.0;
        if result.1.timed_out() {
            node_conf.operation_result = Some(Err(anyhow::anyhow!("timeout")));
        }

        node_conf.operation_result.take().unwrap()
    }

    fn parse_node_infos(
        &self,
        message: &MqttMessage,
        app: &str,
        operation: &AcqFileOperation,
    ) -> Result<Vec<NodeInfo>> {
        match operation {
            AcqFileOperation::Add | AcqFileOperation::Del => {
                let node_infos = serde_json::from_str::<Value>(message.payload())
                    .map_err(anyhow::Error::from)
                    .and_then(|v| {
                        v.get("body")
                            .ok_or_else(|| anyhow::anyhow!("body not exist"))
                            .map(|body_value| {
                                serde_json::from_value::<Vec<NodeInfo>>(body_value.clone())
                                    .map_err(anyhow::Error::from)
                            })
                    });
                match node_infos {
                    Err(e) | Ok(Err(e)) => anyhow::bail!(e),
                    Ok(Ok(node_infos)) => Ok(node_infos),
                }
            }
            AcqFileOperation::Clear => {
                let node_conf = self.node_conf.lock().unwrap();
                Ok(node_conf
                    .node_config
                    .get_all_node_infos(app)
                    .into_iter()
                    .map(|n| (*n).clone())
                    .collect())
            }
        }
    }

    pub fn mqtt_opration_acq_files(
        &self,
        message: MqttMessage,
        mqtt_msg_sender: &mpsc::Sender<MqttMessage>,
        uart_msg_sender: &mpsc::Sender<UartMessage>,
        operation: AcqFileOperation,
    ) -> Result<()> {
        let app = MqttTopic::try_from(message.topic())
            .unwrap()
            .app()
            .to_owned();
        let node_infos = match self.parse_node_infos(&message, app.as_str(), &operation) {
            Err(e) => {
                mqtt_msg_sender.send(MqttMessage::new_with_msg_status_reason(
                    message,
                    "FAILURE",
                    e.to_string(),
                ))?;
                return anyhow::bail!(e);
            }
            Ok(node_infos) => node_infos,
        };

        // 单次处理ACQ_FILES_CHUNK_SIZE个节点
        let result = match operation {
            AcqFileOperation::Add => {
                self.add_acq_files(app.as_str(), &message, node_infos, uart_msg_sender)
            }
            AcqFileOperation::Del | AcqFileOperation::Clear => {
                self.del_acq_files(app.as_str(), &message, node_infos, uart_msg_sender)
            }
        };
        let response = match result {
            Ok(_) => MqttMessage::new_with_msg_body(message, None),
            Err(e) => MqttMessage::new_with_msg_status_reason(message, "FAILURE", e.to_string()),
        };
        mqtt_msg_sender.send(response)?;

        Ok(())
    }
}

impl NodeManage {
    fn uart_add_node_infos(
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
                    if !node_config.add_node_info_exist(app, node_info)? {
                        uart_node_infos.push(node_info.clone())
                    }
                    Ok(uart_node_infos)
                },
            ),
        }
    }

    fn add_chunk_acq_files(
        &self,
        app: &str,
        mut mqtt_req_info: Option<MqttReqInfo>,
        node_infos: &[NodeInfo],
        uart_msg_sender: &mpsc::Sender<UartMessage>,
    ) -> Result<usize> {
        let mut node_config = self.node_conf.lock().unwrap();
        let node_infos = Self::uart_add_node_infos(
            &mut node_config.node_config,
            app,
            node_infos,
            mqtt_req_info.is_none(),
        )?;

        // 无需uart操作
        let node_number = node_infos.len();
        if node_number == 0 {
            return Ok(0);
        }

        let uart_node_infos = node_infos
            .iter()
            .map(|node_info| node_info.to_uart_node_info())
            .collect();
        if let Some(mqtt_req_info) = mqtt_req_info.as_mut() {
            //debug!("mqtt req info: {:?}", mqtt_req_info);
            let extra_data = NodeInfoData { node_infos };
            mqtt_req_info.set_extra_data(Some(Box::new(extra_data)));
        }
        let request = AddNodeRequest::new(uart_node_infos);
        let frame = Frame::new_request(request.into());

        let req_info = ReqInfo::new(&frame, mqtt_req_info);
        uart_msg_sender.send(UartMessage::new(req_info, frame))?;

        self.wait_operation_result(node_config)
            .map(|_| node_number)?;

        Ok(node_number)
    }

    fn add_acq_files(
        &self,
        app: &str,
        message: &MqttMessage,
        node_infos: Vec<NodeInfo>,
        uart_msg_sender: &mpsc::Sender<UartMessage>,
    ) -> Result<()> {
        for (index, nodes) in node_infos.chunks(ACQ_FILES_CHUNK_SIZE).enumerate() {
            match self.add_chunk_acq_files(
                app,
                Some(message.to_mqtt_req_info()),
                nodes,
                uart_msg_sender,
            ) {
                Ok(0) => {
                    info!(
                        "node index[{}-{}) no need to operate uart",
                        index,
                        index + nodes.len()
                    );
                }
                Ok(_) => {}
                Err(e) => {
                    error!("add chunk acq files error: {}", e);
                    return anyhow::bail!(e);
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
        self.mqtt_opration_acq_files(
            message,
            mqtt_msg_sender,
            uart_msg_sender,
            AcqFileOperation::Add,
        )
    }

    pub fn uart_opration_acq_files(
        &self,
        message: UartMessage,
        operation: AcqFileOperation,
    ) -> Result<()> {
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
            let topic = MqttTopic::try_from(mqtt_req_info.topic()).unwrap();
            let extra_data = mqtt_req_info.extra_data().unwrap();
            let node_infos = extra_data.downcast::<NodeInfoData>().unwrap();
            let mut node_conf = self.node_conf.lock().unwrap();
            let result = match operation {
                AcqFileOperation::Add => {
                    node_conf
                        .node_config
                        .add_node_infos_checked(topic.info_target(), node_infos.node_infos);
                    Ok(())
                }
                AcqFileOperation::Del => node_conf
                    .node_config
                    .remove_node_infos_checked(topic.info_target(), &node_infos.node_infos),
                AcqFileOperation::Clear => unreachable!("no clear operation"),
            };
            node_conf.operation_result = Some(result);
            self.cond.notify_one();
        }

        Ok(())
    }

    pub fn uart_add_acq_files(&self, message: UartMessage) -> Result<()> {
        self.uart_opration_acq_files(message, AcqFileOperation::Add)
    }
}

impl NodeManage {
    fn uart_del_node_infos(
        node_config: &mut node_config::NodeConfig,
        app: &str,
        node_infos: &[NodeInfo],
        _is_init: bool,
    ) -> Result<Vec<NodeInfo>> {
        node_infos
            .iter()
            .try_fold(Vec::<NodeInfo>::new(), |mut uart_node_infos, node_info| {
                if node_config.should_remove_node_info(app, node_info) {
                    uart_node_infos.push(node_info.clone());
                }
                Ok(uart_node_infos)
            })
    }

    fn del_chunk_acq_files(
        &self,
        app: &str,
        mut mqtt_req_info: Option<MqttReqInfo>,
        node_infos: &[NodeInfo],
        uart_msg_sender: &mpsc::Sender<UartMessage>,
    ) -> Result<usize> {
        let mut node_config = self.node_conf.lock().unwrap();
        let node_infos = Self::uart_del_node_infos(
            &mut node_config.node_config,
            app,
            node_infos,
            mqtt_req_info.is_none(),
        )?;

        // 无需uart操作
        let node_number = node_infos.len();
        if node_number == 0 {
            return Ok(0);
        }

        let uart_node_infos = node_infos
            .iter()
            .map(|node_info| Address::from(node_info.acq_addr()))
            .collect();
        if let Some(mqtt_req_info) = mqtt_req_info.as_mut() {
            //let extra_data = NodeAddressData {
            //    node_addresses: uart_node_infos.clone(),
            //};
            let extra_data = NodeInfoData { node_infos };
            mqtt_req_info.set_extra_data(Some(Box::new(extra_data)));
        }
        let frame = Frame::new_request(DelNodeRequest::new(uart_node_infos).into());

        let req_info = ReqInfo::new(&frame, mqtt_req_info);
        uart_msg_sender.send(UartMessage::new(req_info, frame))?;

        self.wait_operation_result(node_config)
            .map(|_| node_number)?;

        Ok(node_number)
    }

    fn del_acq_files(
        &self,
        app: &str,
        message: &MqttMessage,
        node_infos: Vec<NodeInfo>,
        uart_msg_sender: &mpsc::Sender<UartMessage>,
    ) -> Result<()> {
        for (index, nodes) in node_infos.chunks(ACQ_FILES_CHUNK_SIZE).enumerate() {
            match self.del_chunk_acq_files(
                app,
                Some(message.to_mqtt_req_info()),
                nodes,
                uart_msg_sender,
            ) {
                Ok(0) => {
                    info!(
                        "node index[{}-{}) no need to operate uart",
                        index,
                        index + nodes.len()
                    );
                }
                Ok(_) => {}
                Err(e) => {
                    error!("del chunk acq files error: {}", e);
                    return anyhow::bail!(e);
                }
            }
        }

        Ok(())
    }

    pub fn mqtt_del_acq_files(
        &self,
        message: MqttMessage,
        mqtt_msg_sender: &mpsc::Sender<MqttMessage>,
        uart_msg_sender: &mpsc::Sender<UartMessage>,
    ) -> Result<()> {
        self.mqtt_opration_acq_files(
            message,
            mqtt_msg_sender,
            uart_msg_sender,
            AcqFileOperation::Del,
        )
    }

    pub fn uart_del_acq_files(&self, message: UartMessage) -> Result<()> {
        self.uart_opration_acq_files(message, AcqFileOperation::Del)
    }

    pub fn mqtt_clear_acq_files(
        &self,
        message: MqttMessage,
        mqtt_msg_sender: &mpsc::Sender<MqttMessage>,
        uart_msg_sender: &mpsc::Sender<UartMessage>,
    ) -> Result<()> {
        self.mqtt_opration_acq_files(
            message,
            mqtt_msg_sender,
            uart_msg_sender,
            AcqFileOperation::Clear,
        )
    }
}
