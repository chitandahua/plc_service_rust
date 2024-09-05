use serde::Serialize;
use std::sync::{mpsc, Arc};

use crate::mqtt_handler::MqttTopicType;
use crate::mqtt_message::{MqttPayload, PayloadBody};
use crate::protocol::app_data::{self, ConfirmResponse, ModuleInfoRequest};
use crate::protocol::Frame;
use crate::request_info::{self, MqttReqInfo, UartMessage};
use crate::service::parse_response::UartResponse;
use crate::service::IntoMqttMessage;
use crate::{MqttMessage, MqttMsgHandler, ReqInfo, Result, APP_NAME};

pub struct ModuleInfo;

impl ModuleInfo {
    pub fn module_info_response(
        message: UartMessage,
        sender: &mpsc::Sender<MqttMessage>,
        uart_msg_sender: &mpsc::Sender<UartMessage>,
    ) -> Result<()> {
        let seq = message.frame.get_seq();
        let response = UartResponse::<app_data::ModuleInfoResponse>::try_from(message.frame)?;

        if message.req_info.is_init() {
            let response_frame = Frame::new_response(seq, None, ConfirmResponse::default());
            let req_info = ReqInfo::new(&response_frame, None, None);
            let _ = uart_msg_sender.send(UartMessage::new(req_info, response_frame));
        } else {
            let mqtt_req_info = message.req_info.into_mqtt_req_info().unwrap();
            let _ = sender.send(response.into_mqtt_message(mqtt_req_info));
        }

        Ok(())
    }

    pub fn mqtt_get_module_info(
        message: MqttMessage,
        uart_msg_sender: &mpsc::Sender<UartMessage>,
    ) -> Result<()> {
        let frame = Frame::new_request(None, ModuleInfoRequest);
        let req_info = ReqInfo::new_with_mqtt(
            &frame,
            message.topic(),
            message.get_token(),
            None,
            Some(Arc::new(request_info::timeout_handler)),
        );
        uart_msg_sender
            .send(UartMessage::new(req_info, frame))
            .unwrap();

        Ok(())
    }

    pub fn init(mqtt_msg_handler: &mut MqttMsgHandler) {
        let topic = format!("{}{}{}", "+/get/request/", APP_NAME, "/modeInfo");
        mqtt_msg_handler.add_topic_filter(topic, MqttTopicType::GetModuleInfo);
    }
}

#[derive(Debug, Serialize)]
pub(crate) struct ModuleInfoResponse {
    #[serde(rename = "communicationMode")]
    communication_mode: String,
    #[serde(rename = "slaveMonitorOvertime")]
    slave_monitor_overtime: String,
    #[serde(rename = "BroadcastMaxOvertime")]
    broadcast_max_overtime: String,
    #[serde(rename = "packageMaxLen")]
    package_max_len: String,
    #[serde(rename = "upgradeMaxPackLen")]
    upgrade_max_pack_len: String,
    #[serde(rename = "upgradeActionWaitTime")]
    upgrade_action_wait_time: String,
    #[serde(rename = "BroadcastDelaySuccess")]
    broadcast_delay_success: String,
    #[serde(rename = "moduleaddr")]
    module_addr: String,
    #[serde(rename = "supportMaxSlaveNum")]
    support_max_slave_num: String,
    #[serde(rename = "supportSlaveNum")]
    support_slave_num: String,
    #[serde(rename = "moduleVerInfo")]
    module_ver_info: String,
}

impl From<app_data::ModuleInfoResponse> for ModuleInfoResponse {
    fn from(module_info_response: app_data::ModuleInfoResponse) -> Self {
        ModuleInfoResponse {
            communication_mode: module_info_response.comm_mode.to_string(),
            slave_monitor_overtime: module_info_response.max_timeout_time.to_string(),
            broadcast_max_overtime: module_info_response.broadcast_cmd_timeout_time.to_string(),
            package_max_len: module_info_response.max_packet_length.to_string(),
            upgrade_max_pack_len: module_info_response.max_packet_per_packet.to_string(),
            upgrade_action_wait_time: module_info_response.upgrade_wait_time.to_string(),
            broadcast_delay_success: "NULL".to_string(),
            module_addr: module_info_response.main_node_addr.to_string(),
            support_max_slave_num: module_info_response.max_node_num.to_string(),
            support_slave_num: module_info_response.current_node_num.to_string(),
            module_ver_info: format!(
                "{}{}-{}-{}",
                module_info_response.factory_code,
                module_info_response.chip_code,
                app_data::ModuleInfoResponse::date_to_string(&module_info_response.version_date),
                module_info_response.version
            ),
        }
    }
}

impl IntoMqttMessage for app_data::ModuleInfoResponse {
    fn into_mqtt_message(self, mqtt_req_info: MqttReqInfo) -> MqttMessage {
        let response = ModuleInfoResponse::from(self);
        let payload = serde_json::to_value(response).unwrap();
        let payload = MqttPayload::new_with_token(
            mqtt_req_info.token(),
            Some(PayloadBody::Nested { body: payload }),
        );
        let mut value = serde_json::to_value(payload).unwrap();
        // modulePlug
        value["modulePlug"] = "1".into();
        MqttMessage::new(mqtt_req_info.topic(), value)
    }
}
