use super::node_config::NodeConfig;
use std::sync::{Arc, Mutex};

pub struct NodeManage {
    node_config: Arc<Mutex<NodeConfig>>,
}
