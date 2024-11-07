mod broadcast;
pub use broadcast::Broadcast;

mod concurrent_meter;
pub use concurrent_meter::ConcurrentMeter;

mod data_transfer;
pub use data_transfer::DataTransfer;

mod debug_method;
pub use debug_method::DebugMethod;

mod device_info;
pub use device_info::DeviceInfo;

mod event_report;
pub use event_report::EventReport;

mod hplc_info;
pub use hplc_info::HplcInfo;

mod identify_area;
pub use identify_area::IdentifyArea;

mod meter_state;
pub use meter_state::MeterState;

mod module_info;
pub use module_info::ModuleInfo;

mod master_address;
pub use master_address::MasterAddress;

mod monitor_node;
pub use monitor_node::MonitorNode;

mod node_config;

mod node_manage;
pub use node_manage::NodeManage;

mod parse_response;
pub use parse_response::UartResponse;

mod plc_init;
pub use plc_init::PlcInit;

mod plc_device;
pub use plc_device::PlcDevice;

mod route_ctrl;
pub use route_ctrl::RouteCtrl;

mod route_data_request;
pub use route_data_request::RouteDataRequest;

use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use timer::Timer;

use crate::mqtt_message::Status;
use crate::protocol::app_data::{ConfirmResponse, DenyResponse};
use crate::request_info::MqttReqInfo;
use crate::{MeterConfig, MqttMessage, MqttMsgHandler, MqttPayload, Result};

#[derive(Clone)]
pub struct ModuleService {
    pub master_address: Arc<MasterAddress>,
    pub node_manage: Arc<NodeManage>,
    pub concurrent_meter: ConcurrentMeter,
    pub device_info: DeviceInfo,
    pub monitor_node: MonitorNode,
    pub route_ctrl: RouteCtrl,
    pub data_transfer: DataTransfer,
    pub meter_state: MeterState,
    pub identify_area: IdentifyArea,
}

impl ModuleService {
    pub fn new(
        timer: Arc<Timer>,
        device_info: DeviceInfo,
        meter_config: &MeterConfig,
    ) -> Result<Self> {
        let metering_state = Arc::new(AtomicBool::new(false));
        Ok(Self {
            master_address: Arc::new(MasterAddress::new()),
            node_manage: Arc::new(NodeManage::new(None)?),
            concurrent_meter: ConcurrentMeter::new(
                &timer,
                meter_config.meter_reading.clone(),
                metering_state.clone(),
            ),
            device_info,
            monitor_node: MonitorNode::new(metering_state.clone()),
            route_ctrl: RouteCtrl::new(timer, meter_config.resume_interval),
            data_transfer: DataTransfer::new(metering_state.clone()),
            meter_state: MeterState::new(),
            identify_area: IdentifyArea::new(),
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
        self.monitor_node.init(mqtt_msg_handler);
        self.route_ctrl.init(mqtt_msg_handler);
        Broadcast::init(mqtt_msg_handler);
        DataTransfer::init(mqtt_msg_handler);
        MeterState::init(mqtt_msg_handler);
        IdentifyArea::init(mqtt_msg_handler);
    }
}

pub trait IntoMqttMessage {
    fn into_mqtt_message(self, mqtt_req_info: MqttReqInfo) -> MqttMessage;
}

impl IntoMqttMessage for () {
    fn into_mqtt_message(self, mqtt_req_info: MqttReqInfo) -> MqttMessage {
        MqttMessage::new_with_req_info_body(mqtt_req_info, None)
    }
}

impl<T> IntoMqttMessage for Result<T>
where
    T: IntoMqttMessage,
{
    fn into_mqtt_message(self, mqtt_req_info: MqttReqInfo) -> MqttMessage {
        match self {
            Ok(t) => t.into_mqtt_message(mqtt_req_info),
            Err(e) => {
                MqttMessage::new_with_req_info_status_reason(mqtt_req_info, Status::Failure, e)
            }
        }
    }
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
