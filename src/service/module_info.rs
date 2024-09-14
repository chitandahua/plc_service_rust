use serde::Serialize;
use std::sync::mpsc;

use crate::mqtt_handler::MqttTopicType;
use crate::mqtt_message::{MqttPayload, PayloadBody};
use crate::protocol::app_data::{
    self, module_id_format_string, ConfirmResponse, MasterIdInfoRequest, MasterIdInfoResponse,
    ModuleInfoRequest,
};
use crate::protocol::{Address, Frame};
use crate::request_info::{MqttReqInfo, UartMessage};
use crate::service::parse_response::{
    mqtt_info_request_uart_handler, mqtt_request_uart_handler, uart_response_mqtt_handler,
    UartResponse,
};
use crate::service::IntoMqttMessage;
use crate::{MqttMessage, MqttMsgHandler, ReqInfo, Result, APP_NAME};

pub struct ModuleInfo;

impl ModuleInfo {
    pub fn slave_module_info_report(
        message: UartMessage,
        uart_msg_sender: &mpsc::Sender<UartMessage>,
    ) -> Result<Address> {
        let seq = message.frame.get_seq();
        let response = UartResponse::<app_data::ModuleInfoResponse>::try_from(message.frame)?;

        match response {
            UartResponse::Deny(_) => unreachable!(),
            UartResponse::Normal(response) => {
                let response_frame = Frame::new_response(seq, None, ConfirmResponse::default());
                let req_info = ReqInfo::new(&response_frame, None);
                let _ = uart_msg_sender.send(UartMessage::new(req_info, response_frame));

                Ok(response.main_node_addr)
            }
        }
    }

    pub fn init_module_info_response(message: UartMessage) -> Result<Address> {
        let response = UartResponse::<app_data::ModuleInfoResponse>::try_from(message.frame)?;

        match response {
            UartResponse::Deny(response) => Err(response.into()),
            UartResponse::Normal(response) => Ok(response.main_node_addr),
        }
    }

    pub fn module_info_response(
        message: UartMessage,
        sender: &mpsc::Sender<MqttMessage>,
    ) -> Result<()> {
        uart_response_mqtt_handler::<app_data::ModuleInfoResponse>(message, sender)
    }

    pub fn get_module_info(
        mqtt_req_info: Option<MqttReqInfo>,
        uart_msg_sender: &mpsc::Sender<UartMessage>,
    ) {
        mqtt_info_request_uart_handler::<ModuleInfoRequest>(
            ModuleInfoRequest,
            mqtt_req_info,
            uart_msg_sender,
        );
    }

    pub fn mqtt_get_module_info(message: MqttMessage, uart_msg_sender: &mpsc::Sender<UartMessage>) {
        mqtt_request_uart_handler::<ModuleInfoRequest>(ModuleInfoRequest, message, uart_msg_sender);
    }

    pub fn mqtt_get_master_id_info(
        message: MqttMessage,
        uart_msg_sender: &mpsc::Sender<UartMessage>,
    ) {
        mqtt_request_uart_handler::<MasterIdInfoRequest>(
            MasterIdInfoRequest,
            message,
            uart_msg_sender,
        );
    }

    pub fn master_id_info_response(
        message: UartMessage,
        sender: &mpsc::Sender<MqttMessage>,
    ) -> Result<()> {
        uart_response_mqtt_handler::<MasterIdInfoResponse>(message, sender)
    }

    pub fn init(mqtt_msg_handler: &mut MqttMsgHandler) {
        use crate::config::SCHEMA_PATH;
        use crate::schema_check;
        let topic = format!("{}{}{}", "+/get/request/", APP_NAME, "/modeInfo");
        let schema =
            schema_check::parse_schema(SCHEMA_PATH.join("get_module_info_schema.json")).ok();
        mqtt_msg_handler.add_topic_filter(topic, MqttTopicType::GetModuleInfo, schema);

        let topic = format!("{}{}{}", "+/get/request/", APP_NAME, "/hostModeID");
        let schema = schema_check::parse_schema(SCHEMA_PATH.join("get_master_id_schema.json")).ok();
        mqtt_msg_handler.add_topic_filter(topic, MqttTopicType::GetMasterIdInfo, schema);
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

#[derive(Debug, Serialize)]
pub(crate) struct ModeIdInfoResponse {
    #[serde(rename = "vendorCode")]
    vendor_code: String,
    #[serde(rename = "modeIDLen")]
    mode_id_len: u8,
    #[serde(rename = "modeIDFormat")]
    mode_id_format: u8,
    #[serde(rename = "modeIDInfo")]
    mode_id_info: String,
}

impl From<MasterIdInfoResponse> for ModeIdInfoResponse {
    fn from(master_id_info_response: MasterIdInfoResponse) -> Self {
        ModeIdInfoResponse {
            vendor_code: master_id_info_response.factory_code,
            mode_id_len: master_id_info_response.module_id_length,
            mode_id_format: master_id_info_response.module_id_format as u8,
            mode_id_info: module_id_format_string(
                master_id_info_response.module_id_format,
                &master_id_info_response.module_id,
            ),
        }
    }
}

impl IntoMqttMessage for MasterIdInfoResponse {
    fn into_mqtt_message(self, mqtt_req_info: MqttReqInfo) -> MqttMessage {
        MqttMessage::new_with_req_info_body(
            mqtt_req_info,
            Some(PayloadBody::Nested {
                body: serde_json::to_value(ModeIdInfoResponse::from(self)).unwrap(),
            }),
        )
    }
}
