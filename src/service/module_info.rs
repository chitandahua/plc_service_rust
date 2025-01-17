use serde::Serialize;
use std::sync::{mpsc, OnceLock};

use crate::mqtt_handler::MqttTopicType;
use crate::mqtt_message::{MqttPayload, PayloadBody};
use crate::protocol::app_data::{
    self, date_to_string, module_id_format_string, CommModuleInfoRequest, CommModuleInfoResponse,
    ConfirmResponse, MasterIdInfoRequest, MasterIdInfoResponse, ModuleInfoRequest,
};
use crate::protocol::{Address, Frame};
use crate::request_info::{MqttReqInfo, UartMessage};
use crate::service::parse_response::{
    mqtt_info_request_uart_handler, mqtt_request_uart_handler, uart_response_handler, UartResponse,
};
use crate::service::IntoMqttMessage;
use crate::{
    impl_into_mqtt_message, register_mqtt_request_topics, MqttMessage, MqttMsgHandler, ReqInfo,
    Result,
};

pub struct ModuleInfo;

pub static MODULE_INFO: OnceLock<app_data::ModuleInfoResponse> = OnceLock::new();

impl ModuleInfo {
    pub fn slave_module_info_report(
        message: UartMessage,
        uart_msg_sender: &mpsc::Sender<UartMessage>,
    ) -> Result<app_data::ModuleInfoResponse> {
        let seq = message.frame.get_seq();
        let response = UartResponse::<app_data::ModuleInfoResponse>::try_from(message.frame)?;

        match response {
            UartResponse::Deny(_) => unreachable!(),
            UartResponse::Normal(response) => {
                let response_frame = Frame::new_response(seq, None, ConfirmResponse::default());
                let req_info = ReqInfo::new(&response_frame, None);
                let _ = uart_msg_sender.send(UartMessage::new(req_info, response_frame));

                Ok(response)
            }
        }
    }

    pub fn init_module_info_response(message: UartMessage) -> Result<Address> {
        let response = UartResponse::<app_data::ModuleInfoResponse>::try_from(message.frame)?;

        match response {
            UartResponse::Deny(response) => Err(response.into()),
            UartResponse::Normal(response) => {
                let address = response.main_node_addr.clone();
                MODULE_INFO.get_or_init(move || response);
                Ok(address)
            }
        }
    }

    pub fn auto_reading_meter() -> u8 {
        MODULE_INFO
            .get()
            .map_or(0, |info| info.metering_mode & 0x10)
    }

    pub fn module_info_response(
        message: UartMessage,
        sender: &mpsc::Sender<MqttMessage>,
    ) -> Result<()> {
        uart_response_handler::<app_data::ModuleInfoResponse, ModuleInfoResponse>(message, sender)
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
        uart_response_handler::<MasterIdInfoResponse, ModeIdInfoResponse>(message, sender)
    }

    pub fn init(mqtt_msg_handler: &mut MqttMsgHandler) {
        register_mqtt_request_topics!(
            mqtt_msg_handler,
            (
                "get",
                "moduleInfo",
                MqttTopicType::GetModuleInfo,
                "get_module_info_schema.json"
            ),
            (
                "get",
                "hostModeID",
                MqttTopicType::GetMasterIdInfo,
                "get_master_id_schema.json"
            ),
            (
                "get",
                "hplcFreq",
                MqttTopicType::GetHplcFreq,
                "get_hplc_freq_schema.json"
            ),
            (
                "get",
                "modeInfo",
                MqttTopicType::GetModeInfo,
                "get_mode_info_schema.json"
            )
        )
    }
}

#[derive(Debug, Serialize)]
struct MqttModeInfoResponse {
    #[serde(rename = "factory")]
    factory_code: String,
    #[serde(rename = "moduleVendorCode")]
    module_vendor_code: String,
    #[serde(rename = "softDate")]
    soft_date: String,
    #[serde(rename = "softVer")]
    soft_verion: String,
}

impl From<CommModuleInfoResponse> for MqttModeInfoResponse {
    fn from(comm_module_info_response: CommModuleInfoResponse) -> Self {
        MqttModeInfoResponse {
            factory_code: comm_module_info_response.factory_code,
            module_vendor_code: comm_module_info_response.chip_code,
            soft_date: date_to_string(&comm_module_info_response.version_date),
            soft_verion: comm_module_info_response.version.to_string(),
        }
    }
}

impl_into_mqtt_message!(MqttModeInfoResponse, nested);

impl ModuleInfo {
    pub fn mqtt_get_mode_info(message: MqttMessage, uart_msg_sender: &mpsc::Sender<UartMessage>) {
        mqtt_request_uart_handler::<CommModuleInfoRequest>(
            CommModuleInfoRequest,
            message,
            uart_msg_sender,
        );
    }

    pub fn uart_get_mode_info_response(
        message: UartMessage,
        sender: &mpsc::Sender<MqttMessage>,
    ) -> Result<()> {
        uart_response_handler::<CommModuleInfoResponse, MqttModeInfoResponse>(message, sender)
    }
}

#[derive(Debug, Serialize)]
struct HplcFrequencyResponse {
    #[serde(rename = "hplcFreq")]
    hplc_freq: u8,
}

impl From<app_data::GetHplcFreqResponse> for HplcFrequencyResponse {
    fn from(hplc_freq_response: app_data::GetHplcFreqResponse) -> Self {
        HplcFrequencyResponse {
            hplc_freq: hplc_freq_response.frequency,
        }
    }
}

impl_into_mqtt_message!(HplcFrequencyResponse, flat);

impl ModuleInfo {
    pub fn mqtt_get_hplc_freq(message: MqttMessage, uart_msg_sender: &mpsc::Sender<UartMessage>) {
        mqtt_request_uart_handler::<app_data::GetHplcFreqRequest>(
            app_data::GetHplcFreqRequest,
            message,
            uart_msg_sender,
        );
    }

    pub fn uart_get_hplc_freq_response(
        message: UartMessage,
        sender: &mpsc::Sender<MqttMessage>,
    ) -> Result<()> {
        uart_response_handler::<app_data::GetHplcFreqResponse, HplcFrequencyResponse>(
            message, sender,
        )
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
            module_addr: module_info_response.main_node_addr.to_string(),
            support_max_slave_num: module_info_response.max_node_num.to_string(),
            support_slave_num: module_info_response.current_node_num.to_string(),
            module_ver_info: format!(
                "{}{}-{}-{}",
                module_info_response.factory_code,
                module_info_response.chip_code,
                date_to_string(&module_info_response.version_date),
                module_info_response.version
            ),
        }
    }
}

impl IntoMqttMessage for ModuleInfoResponse {
    fn into_mqtt_message(self, mqtt_req_info: MqttReqInfo) -> MqttMessage {
        let payload = serde_json::to_value(self).unwrap();
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
struct ModeIdInfoResponse {
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

impl_into_mqtt_message!(ModeIdInfoResponse, nested);
