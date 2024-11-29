use serde::Serialize;

use crate::mqtt_handler::MqttTopicType;
use crate::mqtt_message::{MqttMessage, PayloadBody};
use crate::protocol::app_data::{CurrentStatus, RunningStatusRequest, RunningStatusResponse};
use crate::request_info::{MqttReqInfo, UartMessage};
use crate::service::parse_response::{mqtt_request_uart_handler, UartResponse};
use crate::service::IntoMqttMessage;
use crate::{ModuleInfo, MqttMsgHandler, Result, APP_NAME};

use chrono::DateTime;
use std::sync::{mpsc, Arc, Mutex};

struct AppStat {
    last_receive_time: DateTime<chrono::Local>,
    init_data_count: u32,
    init_param_count: u32,
    reset_module_count: u32,
    reset_router_count: u32,
}

#[derive(Clone)]
pub struct MeterState {
    state: Arc<Mutex<AppStat>>,
}

impl MeterState {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(AppStat {
                last_receive_time: chrono::Local::now(),
                init_data_count: 0,
                init_param_count: 0,
                reset_module_count: 0,
                reset_router_count: 0,
            })),
        }
    }

    pub fn init(mqtt_msg_handler: &mut MqttMsgHandler) {
        use crate::config::SCHEMA_PATH;
        use crate::schema_check;
        let topic = format!("{}{}{}", "+/get/request/", APP_NAME, "/meteringState");
        let schema =
            schema_check::parse_schema(SCHEMA_PATH.join("get_metering_state_schema.json")).ok();
        mqtt_msg_handler.add_topic_filter(topic, MqttTopicType::MeteringState, schema);
    }

    pub fn mqtt_get_metering_state(
        &self,
        message: MqttMessage,
        uart_msg_sender: &mpsc::Sender<UartMessage>,
    ) {
        mqtt_request_uart_handler::<RunningStatusRequest>(
            RunningStatusRequest,
            message,
            uart_msg_sender,
        );
    }

    pub fn uart_metering_state_response(
        &self,
        message: UartMessage,
        mqtt_msg_sender: &mpsc::Sender<MqttMessage>,
    ) -> Result<()> {
        let response = UartResponse::<RunningStatusResponse>::try_from(message.frame)?;
        let mqtt_req_info = message.req_info.into_mqtt_req_info().unwrap();
        let response_msg = match response {
            UartResponse::Normal(res) => {
                let res = MeteringStateResponse::new(self, res);
                res.into_mqtt_message(mqtt_req_info)
            }
            UartResponse::Deny(deny) => deny.into_mqtt_message(mqtt_req_info),
        };

        mqtt_msg_sender.send(response_msg).unwrap();

        Ok(())
    }

    pub fn _init_data(&self) {
        let mut state = self.state.lock().unwrap();
        state.init_data_count += 1;
    }

    pub fn init_param(&self) {
        let mut state = self.state.lock().unwrap();
        state.init_param_count += 1;
    }

    pub fn reset_module(&self) {
        let mut state = self.state.lock().unwrap();
        state.reset_module_count += 1;
    }

    pub fn _reset_router(&self) {
        let mut state = self.state.lock().unwrap();
        state.reset_router_count += 1;
    }

    pub fn receive_message(&self) {
        let mut state = self.state.lock().unwrap();
        state.last_receive_time = chrono::Local::now();
    }
}

#[derive(Debug, Serialize)]
struct MeteringStateResponse {
    #[serde(rename = "lastReceived")]
    last_receive_time: String,
    #[serde(rename = "ctrlstat")]
    ctrl_stat: u8,
    #[serde(rename = "searchstat")]
    search_stat: u8,
    #[serde(rename = "autoReading")]
    auto_reading: u8,
    #[serde(rename = "initDataCount")]
    init_data_count: u32,
    #[serde(rename = "initParamCount")]
    init_param_count: u32,
    #[serde(rename = "resetModuleCount")]
    reset_module_count: u32,
    #[serde(rename = "resetRouterCount")]
    reset_router_count: u32,
    #[serde(rename = "identifyAreaStat")]
    identify_area_stat: u8,
}

impl MeteringStateResponse {
    fn new(meter_state: &MeterState, response: RunningStatusResponse) -> Self {
        let state = meter_state.state.lock().unwrap();
        Self {
            last_receive_time: state
                .last_receive_time
                .naive_local()
                .format("%Y-%m-%d %H:%M:%S")
                .to_string(),
            ctrl_stat: (response.current_status() == CurrentStatus::Metering) as u8,
            search_stat: (response.current_status() == CurrentStatus::Searching) as u8,
            auto_reading: ModuleInfo::auto_reading_meter(),
            init_data_count: state.init_data_count,
            init_param_count: state.init_data_count,
            reset_module_count: state.reset_module_count,
            reset_router_count: state.reset_router_count,
            identify_area_stat: response.work_status.area_identify_status,
        }
    }
}

impl IntoMqttMessage for MeteringStateResponse {
    fn into_mqtt_message(self, mqtt_req_info: MqttReqInfo) -> MqttMessage {
        MqttMessage::new_with_req_info_body(
            mqtt_req_info,
            Some(PayloadBody::Flat(serde_json::to_value(self).unwrap())),
        )
    }
}
