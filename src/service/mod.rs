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
pub use parse_response::{IntoMqttMessage, UartResponse};

use std::sync::Arc;

#[derive(Clone)]
pub struct ModuleService {
    pub master_address: Arc<MasterAddress>,
    pub node_manage: Arc<NodeManage>,
}

impl ModuleService {
    pub fn new(master_address: MasterAddress, node_manage: NodeManage) -> Self {
        Self {
            master_address: Arc::new(master_address),
            node_manage: Arc::new(node_manage),
        }
    }
}
