mod concurrent_meter;
pub use concurrent_meter::ConcurrentMeter;

mod debug_method;
pub use debug_method::DebugMethod;

mod device_info;
pub use device_info::DeviceInfo;

mod event_report;
pub use event_report::EventReport;

mod hplc_info;
pub use hplc_info::HplcInfo;

mod module_info;
pub use module_info::ModuleInfo;

mod master_address;
pub use master_address::MasterAddress;

mod node_config;

mod node_manage;
pub use node_manage::NodeManage;

mod parse_response;
pub use parse_response::UartResponse;

mod plc_init;
pub use plc_init::PlcInit;

mod plc_device;
pub use plc_device::PlcDevice;

mod route_data_request;
pub use route_data_request::RouteDataRequest;

use std::sync::Arc;
use timer::Timer;

use crate::protocol::app_data::{ConfirmResponse, DenyResponse};
use crate::request_info::MqttReqInfo;
use crate::{MeterConfig, MqttMessage, MqttMsgHandler, MqttPayload, Result};

#[derive(Clone)]
pub struct ModuleService {
    pub master_address: Arc<MasterAddress>,
    pub node_manage: Arc<NodeManage>,
    pub concurrent_meter: ConcurrentMeter,
    pub device_info: DeviceInfo,
}

impl ModuleService {
    pub fn new(
        timer: Arc<Timer>,
        device_info: DeviceInfo,
        meter_config: &MeterConfig,
    ) -> Result<Self> {
        Ok(Self {
            master_address: Arc::new(MasterAddress::new()),
            node_manage: Arc::new(NodeManage::new(None, meter_config.uart_timeout as u64)?),
            concurrent_meter: ConcurrentMeter::new(&timer, meter_config.meter_reading.clone()),
            device_info,
        })
    }

    pub fn init(&self, mqtt_msg_handler: &mut MqttMsgHandler) {
        ModuleInfo::init(mqtt_msg_handler);
        self.master_address.init(mqtt_msg_handler);
        self.node_manage.init(mqtt_msg_handler);
        self.concurrent_meter.init(mqtt_msg_handler);
        HplcInfo::init(mqtt_msg_handler);
        DebugMethod::init(mqtt_msg_handler);
        self.device_info.init(mqtt_msg_handler);
    }
}

pub trait IntoMqttMessage {
    fn into_mqtt_message(self, mqtt_req_info: MqttReqInfo) -> MqttMessage;
}

impl IntoMqttMessage for ConfirmResponse {
    fn into_mqtt_message(self, mqtt_req_info: MqttReqInfo) -> MqttMessage {
        let payload = MqttPayload::new_with_token(mqtt_req_info.token(), None);
        MqttMessage::new(mqtt_req_info.topic(), payload)
    }
}

impl IntoMqttMessage for DenyResponse {
    fn into_mqtt_message(self, mqtt_req_info: MqttReqInfo) -> MqttMessage {
        let payload = MqttPayload::new_with_token_result(mqtt_req_info.token(), Err(self.into()));
        MqttMessage::new(mqtt_req_info.topic(), payload)
    }
}
