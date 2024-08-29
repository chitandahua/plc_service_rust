use serde::Serialize;
use std::sync::mpsc;

use crate::mqtt_handler::MqttTopicType;
use crate::mqtt_message::MqttPayload;
use crate::protocol::app_data::{self, AppData, ModuleInfoRequest};
use crate::protocol::Frame;
use crate::request_info::UartMessage;
use crate::uart_handler::UartMsgHandler;
use crate::APP_NAME;
use crate::{MqttMessage, MqttMsgHandler, ReqInfo, Result};

pub struct ModuleInfo;

impl ModuleInfo {
    pub fn module_info_response(
        message: UartMessage,
        sender: &mpsc::Sender<MqttMessage>,
    ) -> Result<()> {
        let app_data: AppData = message.frame.into_app_data();
        let frame = app_data::ModuleInfoResponse::try_from(app_data)?;

        let response = ModuleInfoResponse::from(frame);

        if message.req_info.is_init() {
            Ok(())
        } else {
            let mqtt_req_info = message.req_info.into_mqtt_req_info().unwrap();
            let payload = serde_json::to_value(response)?;
            let payload = MqttPayload::new_with_token(mqtt_req_info.token(), Some(payload));
            let message = MqttMessage::new(mqtt_req_info.topic(), payload.to_string());
            let _ = sender.send(message);
            Ok(())
        }
    }

    pub fn mqtt_get_module_info(
        message: MqttMessage,
        uart_msg_sender: &mpsc::Sender<UartMessage>,
    ) -> Result<()> {
        let frame = Frame::new_request(ModuleInfoRequest.into());
        let req_info = ReqInfo::new_with_mqtt(&frame, message.topic(), message.get_token(), None);
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
struct ModuleInfoResponse {
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
