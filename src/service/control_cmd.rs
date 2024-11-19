use serde::Deserialize;

use crate::mqtt_handler::MqttTopicType;
use crate::protocol::app_data::{ConfirmResponse, HplcFrequencySetRequest, SlaveReportRequest};

use crate::request_info::UartMessage;
use crate::{MqttMessage, MqttMsgHandler, Result, APP_NAME};

use crate::service::parse_response::{mqtt_request_uart_handler, uart_response_mqtt_handler};

use std::sync::mpsc;

pub struct ControlCmd;

#[derive(Debug, Deserialize)]
struct MqttHplcFrequencyRequest {
    #[serde(rename = "hplcFreq")]
    frequency: u8,
}

impl From<MqttHplcFrequencyRequest> for HplcFrequencySetRequest {
    fn from(req: MqttHplcFrequencyRequest) -> Self {
        Self::new(req.frequency)
    }
}

#[derive(Debug, Deserialize)]
struct MqttRefuseSlaveReportRequest {
    switch: u8,
}

impl From<MqttRefuseSlaveReportRequest> for SlaveReportRequest {
    fn from(req: MqttRefuseSlaveReportRequest) -> Self {
        Self::new(req.switch)
    }
}

impl ControlCmd {
    pub fn init(mqtt_msg_handler: &mut MqttMsgHandler) {
        use crate::config::SCHEMA_PATH;
        use crate::schema_check;
        let topic = format!("{}{}{}", "+/set/request/", APP_NAME, "/hplcFreq");
        let schema =
            schema_check::parse_schema(SCHEMA_PATH.join("hplc_frequency_schema.json")).ok();
        mqtt_msg_handler.add_topic_filter(topic, MqttTopicType::HplcFrequency, schema);

        let topic = format!("{}{}{}", "+/set/request/", APP_NAME, "/refuseNodeReportCfg");
        let schema =
            schema_check::parse_schema(SCHEMA_PATH.join("refuse_slave_report_schema.json")).ok();
        mqtt_msg_handler.add_topic_filter(topic, MqttTopicType::RefuseSlaveReport, schema);
    }

    pub fn mqtt_set_hplc_frequency(
        message: MqttMessage,
        uart_msg_sender: &mpsc::Sender<UartMessage>,
    ) {
        let req = serde_json::from_str::<MqttHplcFrequencyRequest>(message.payload()).unwrap();
        mqtt_request_uart_handler::<HplcFrequencySetRequest>(
            HplcFrequencySetRequest::from(req),
            message,
            uart_msg_sender,
        );
    }

    pub fn mqtt_refuse_slave_report(
        message: MqttMessage,
        uart_msg_sender: &mpsc::Sender<UartMessage>,
    ) {
        let req = serde_json::from_str::<MqttRefuseSlaveReportRequest>(message.payload()).unwrap();
        mqtt_request_uart_handler::<SlaveReportRequest>(
            SlaveReportRequest::from(req),
            message,
            uart_msg_sender,
        );
    }

    pub fn uart_hplc_frequency_response(
        message: UartMessage,
        sender: &mpsc::Sender<MqttMessage>,
    ) -> Result<()> {
        uart_response_mqtt_handler::<ConfirmResponse>(message, sender)
    }

    pub fn uart_refuse_slave_report_response(
        message: UartMessage,
        sender: &mpsc::Sender<MqttMessage>,
    ) -> Result<()> {
        uart_response_mqtt_handler::<ConfirmResponse>(message, sender)
    }
}
