mod concurrent_meter;
pub use concurrent_meter::ConcurrentMeter;

mod device_info;
pub use device_info::DeviceInfo;

mod module_info;
pub use module_info::ModuleInfo;

mod master_address;
pub use master_address::MasterAddress;

mod node_config;
pub use node_config::NodeInfo;

mod node_manage;
pub use node_manage::NodeManage;

mod parse_response;
pub use parse_response::UartResponse;

use std::sync::Arc;

use crate::protocol::app_data::{ConfirmResponse, DenyResponse};
use crate::request_info::MqttReqInfo;
use crate::{MqttMessage, MqttPayload};

#[derive(Clone)]
pub struct ModuleService {
    pub master_address: Arc<MasterAddress>,
    pub node_manage: Arc<NodeManage>,
    pub concurrent_meter: ConcurrentMeter,
}

impl ModuleService {
    pub fn new(
        master_address: MasterAddress,
        node_manage: NodeManage,
        concurrent_meter: ConcurrentMeter,
    ) -> Self {
        Self {
            master_address: Arc::new(master_address),
            node_manage: Arc::new(node_manage),
            concurrent_meter,
        }
    }
}

pub trait IntoMqttMessage {
    fn into_mqtt_message(self, mqtt_req_info: MqttReqInfo) -> MqttMessage;
}

impl IntoMqttMessage for ConfirmResponse {
    fn into_mqtt_message(self, mqtt_req_info: MqttReqInfo) -> MqttMessage {
        let payload = MqttPayload::new_with_token_status_reason(mqtt_req_info.token(), "OK", "OK");
        MqttMessage::new(mqtt_req_info.topic(), payload)
    }
}

impl IntoMqttMessage for DenyResponse {
    fn into_mqtt_message(self, mqtt_req_info: MqttReqInfo) -> MqttMessage {
        let payload = MqttPayload::new_with_token_status_reason(
            mqtt_req_info.token(),
            "FAILURE",
            self.error_code(),
        );
        MqttMessage::new(mqtt_req_info.topic(), payload)
    }
}
