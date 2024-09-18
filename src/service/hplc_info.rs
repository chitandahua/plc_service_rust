use serde::{Deserialize, Serialize};
use std::sync::mpsc;

use crate::mqtt_handler::MqttTopicType;
use crate::mqtt_message::PayloadBody;
use crate::protocol::app_data::{ChipInfoRequest, ChipInfoResponse, IdInfoRequest, IdInfoResponse};
use crate::request_info::{MqttReqInfo, UartMessage};
use crate::service::parse_response::{mqtt_request_uart_handler, uart_response_mqtt_handler};
use crate::service::IntoMqttMessage;
use crate::{MqttMessage, MqttMsgHandler, Result, APP_NAME};

pub struct ChipInfo;

#[derive(Debug, Deserialize)]
struct HplcInfoRequest {
    #[serde(rename = "startIndex")]
    start_index: u16,
    #[serde(rename = "nodeNumber")]
    node_number: u8,
}

impl ChipInfo {
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

impl ChipInfo {
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
