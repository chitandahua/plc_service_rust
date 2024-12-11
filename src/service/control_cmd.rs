use serde::Deserialize;

use crate::mqtt_handler::MqttTopicType;
use crate::protocol::app_data::{ConfirmResponse, HplcFrequencySetRequest, SlaveReportRequest};

use crate::request_info::UartMessage;
use crate::{register_mqtt_request_topics, MqttMessage, MqttMsgHandler, Result};

use crate::service::parse_response::{mqtt_request_handler, uart_response_handler};

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
        register_mqtt_request_topics!(
            mqtt_msg_handler,
            (
                "set",
                "hplcFreq",
                MqttTopicType::HplcFrequency,
                "hplc_frequency_schema.json"
            ),
            (
                "set",
                "refuseNodeReportCfg",
                MqttTopicType::RefuseSlaveReport,
                "refuse_slave_report_schema.json"
            ),
        )
    }

    pub fn mqtt_set_hplc_frequency(
        message: MqttMessage,
        uart_msg_sender: &mpsc::Sender<UartMessage>,
    ) {
        mqtt_request_handler::<HplcFrequencySetRequest, MqttHplcFrequencyRequest>(
            message,
            uart_msg_sender,
        );
    }

    pub fn mqtt_refuse_slave_report(
        message: MqttMessage,
        uart_msg_sender: &mpsc::Sender<UartMessage>,
    ) {
        mqtt_request_handler::<SlaveReportRequest, MqttRefuseSlaveReportRequest>(
            message,
            uart_msg_sender,
        );
    }

    pub fn uart_hplc_frequency_response(
        message: UartMessage,
        sender: &mpsc::Sender<MqttMessage>,
    ) -> Result<()> {
        uart_response_handler::<ConfirmResponse, ()>(message, sender)
    }

    pub fn uart_refuse_slave_report_response(
        message: UartMessage,
        sender: &mpsc::Sender<MqttMessage>,
    ) -> Result<()> {
        uart_response_handler::<ConfirmResponse, ()>(message, sender)
    }
}
