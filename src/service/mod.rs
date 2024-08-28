mod device_info;
pub use device_info::DeviceInfo;

mod module_info;
pub use module_info::ModuleInfo;

mod master_address;
pub use master_address::MasterAddress;

mod node_config;

mod node_manage;
pub use node_manage::NodeManage;

mod parse_response;
pub use parse_response::{IntoMqttMessage, UartResponse};

use std::sync::Arc;

#[derive(Clone)]
pub struct ModuleService {
    pub master_address: Arc<MasterAddress>,
}

impl ModuleService {
    pub fn new(master_address: Arc<MasterAddress>) -> Self {
        Self { master_address }
    }
}
