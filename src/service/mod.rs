mod broadcast;
pub use broadcast::Broadcast;

mod concurrent_meter;
pub use concurrent_meter::ConcurrentMeter;

mod control_cmd;
pub use control_cmd::ControlCmd;

mod data_transfer;
pub use data_transfer::DataTransfer;

mod debug_method;
pub use debug_method::DebugMethod;

mod device_info;
pub use device_info::DeviceInfo;

mod event_report;
pub use event_report::EventReport;

mod file_upgrade;
pub use file_upgrade::FileUpgrade;

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
use threadpool::ThreadPool;

use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use timer::Timer;

use crate::mqtt_message::Status;
use crate::protocol::app_data::{ConfirmResponse, DenyResponse};
use crate::request_info::MqttReqInfo;
use crate::{MeterConfig, MqttMessage, MqttMsgHandler, Result};

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
    pub hplc_info: HplcInfo,
    pub thread_pool: ThreadPool,
    pub file_upgrade: FileUpgrade,
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
            hplc_info: HplcInfo::new(),
            thread_pool: ThreadPool::new(2),
            file_upgrade: FileUpgrade::new(),
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
        ControlCmd::init(mqtt_msg_handler);
        PlcInit::init(mqtt_msg_handler);
        FileUpgrade::init(mqtt_msg_handler);
    }
}

pub trait IntoMqttMessage {
    fn into_mqtt_message(self, mqtt_req_info: MqttReqInfo) -> MqttMessage;
}

impl From<ConfirmResponse> for () {
    fn from(_value: ConfirmResponse) -> Self {}
}

impl IntoMqttMessage for () {
    fn into_mqtt_message(self, mqtt_req_info: MqttReqInfo) -> MqttMessage {
        MqttMessage::new_with_req_info_body(mqtt_req_info, None)
    }
}

impl IntoMqttMessage for crate::Error {
    fn into_mqtt_message(self, mqtt_req_info: MqttReqInfo) -> MqttMessage {
        MqttMessage::new_with_req_info_status_reason(mqtt_req_info, Status::Failure, self)
    }
}

impl<T> IntoMqttMessage for Result<T>
where
    T: IntoMqttMessage,
{
    fn into_mqtt_message(self, mqtt_req_info: MqttReqInfo) -> MqttMessage {
        match self {
            Ok(t) => t.into_mqtt_message(mqtt_req_info),
            Err(e) => e.into_mqtt_message(mqtt_req_info),
        }
    }
}

impl From<DenyResponse> for anyhow::Error {
    fn from(value: DenyResponse) -> Self {
        anyhow::anyhow!(value.error_code())
    }
}

impl IntoMqttMessage for DenyResponse {
    fn into_mqtt_message(self, mqtt_req_info: MqttReqInfo) -> MqttMessage {
        let err: crate::Error = self.into();
        err.into_mqtt_message(mqtt_req_info)
    }
}

#[macro_export]
macro_rules! impl_into_mqtt_message {
    ($type:ty, nested) => {
        impl IntoMqttMessage for $type {
            fn into_mqtt_message(self, mqtt_req_info: MqttReqInfo) -> MqttMessage {
                MqttMessage::new_with_req_info_body(
                    mqtt_req_info,
                    Some(PayloadBody::Nested {
                        body: serde_json::to_value(self).unwrap(),
                    }),
                )
            }
        }
    };

    ($type:ty, flat) => {
        impl IntoMqttMessage for $type {
            fn into_mqtt_message(self, mqtt_req_info: MqttReqInfo) -> MqttMessage {
                MqttMessage::new_with_req_info_body(
                    mqtt_req_info,
                    Some(PayloadBody::Flat(serde_json::to_value(self).unwrap())),
                )
            }
        }
    };
}
