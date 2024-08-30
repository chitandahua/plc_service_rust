use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Weak};
use thiserror::Error;
use tracing::{debug, error, info};
use tracing_subscriber::field::debug;

use crate::protocol::app_data;
use crate::Result;

#[derive(Debug, PartialEq, Error)]
enum NodeConfigError {
    #[error("acq addr {0} not found")]
    NotFound(String),
    #[error("pro type mismatch, expected {0}, got {1}")]
    TypeMismatch(String, String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct NodeInfo {
    #[serde(rename = "acqAddr")]
    acq_addr: String,
    #[serde(rename = "proType")]
    pro_type: String,
}

impl NodeInfo {
    pub fn new(addr: String, type_: String) -> Self {
        NodeInfo {
            acq_addr: addr,
            pro_type: type_,
        }
    }

    pub fn to_uart_node_info(&self) -> app_data::NodeInfo {
        app_data::NodeInfo::new(
            self.acq_addr.as_str().into(),
            self.pro_type.parse().unwrap(),
        )
    }

    pub fn acq_addr(&self) -> &str {
        &self.acq_addr
    }
}

impl From<app_data::NodeDetail> for NodeInfo {
    fn from(node: app_data::NodeDetail) -> Self {
        NodeInfo::new(
            node.src_addr.to_string(),
            format!("{:02x}", node.comm_protocol_type),
        )
    }
}

struct GlobalNodeConfig {
    global_data: HashMap<String, Weak<NodeInfo>>,
}

impl GlobalNodeConfig {
    pub fn new() -> Self {
        GlobalNodeConfig {
            global_data: HashMap::new(),
        }
    }

    pub fn add_node_info(&mut self, node: &Arc<NodeInfo>) {
        self.global_data
            .insert(node.acq_addr.clone(), Arc::downgrade(node));
    }

    pub fn remove_node_info(&mut self, node: &NodeInfo) -> Result<()> {
        match self.global_data.remove(&node.acq_addr) {
            Some(_) => Ok(()),
            None => anyhow::bail!(NodeConfigError::NotFound(node.acq_addr.clone())),
        }
    }

    pub fn size(&self) -> usize {
        self.global_data.len()
    }

    pub fn get_node_info(&self, acq_addr: &str) -> Option<Arc<NodeInfo>> {
        self.global_data.get(acq_addr).and_then(Weak::upgrade)
    }

    pub fn get_all_node_infos(&self) -> Vec<Arc<NodeInfo>> {
        self.global_data
            .values()
            .filter_map(Weak::upgrade)
            .collect()
    }
}

pub(crate) struct NodeConfig {
    node_data: HashMap<String, Vec<Arc<NodeInfo>>>,
    global_node_config: GlobalNodeConfig,
}

impl NodeConfig {
    pub fn new() -> Self {
        NodeConfig {
            node_data: HashMap::new(),
            global_node_config: GlobalNodeConfig::new(),
        }
    }

    pub fn add_node_info_exist(&mut self, app: &str, node: &NodeInfo) -> Result<bool> {
        let app_data = self
            .node_data
            .entry(app.to_string())
            .or_insert_with(Vec::new);

        if let Some(existing) = app_data.iter().find(|n| n.acq_addr == node.acq_addr) {
            //existing.pro_type = node.pro_type.clone();
            //type不同不允许覆盖
            anyhow::ensure!(
                existing.pro_type == node.pro_type,
                NodeConfigError::TypeMismatch(node.pro_type.clone(), existing.pro_type.clone())
            );
            info!("node {} already exist", existing.acq_addr);
        } else if let Some(existing) = self
            .global_node_config
            .get_node_info(node.acq_addr.as_str())
        {
            anyhow::ensure!(
                existing.pro_type == node.pro_type,
                NodeConfigError::TypeMismatch(node.pro_type.clone(), existing.pro_type.clone())
            );
            //self.add_config(&existing)?; // TODO
            app_data.push(existing);
            info!(
                "node {} already exist in other app, just increase ref cnt",
                node.acq_addr
            );
        } else {
            return Ok(false);
        }

        Ok(true)
    }

    pub fn add_node_info_checked(&mut self, app: &str, node: NodeInfo) {
        debug!("add node {} for app {}", &node.acq_addr, app);
        let node = Arc::new(node);
        self.global_node_config.add_node_info(&node);
        self.node_data.get_mut(app).unwrap().push(node);
    }

    pub fn add_node_info(&mut self, app: &str, node: NodeInfo) -> Result<()> {
        if false == self.add_node_info_exist(app, &node)? {
            self.add_node_info_checked(app, node);
        }

        Ok(())
    }

    pub fn should_remove_node_info(&mut self, app: &str, node: &NodeInfo) -> bool {
        let app_data = match self.node_data.get_mut(app) {
            Some(data) => data,
            None => return false,
        };

        let pos = match app_data.iter().position(|n| n.acq_addr == node.acq_addr) {
            Some(p) => p,
            None => return false,
        };

        debug!(
            "Attempting to remove node {} for app {}",
            &node.acq_addr, app
        );

        let count = Arc::strong_count(&app_data[pos]);
        if count == 1 {
            // 只有自己引用
            true
        } else {
            // 如果还有其他引用，只从当前 app_data 中移除
            debug!(
                "Node {} still exists in other app(s), just removing from current app",
                &node.acq_addr
            );
            app_data.remove(pos);
            false
        }
    }

    pub fn remove_node_info_checked(&mut self, app: &str, node: &NodeInfo) -> Result<()> {
        let app_data = self.node_data.get_mut(app).unwrap();
        let pos = app_data
            .iter()
            .position(|n| n.acq_addr == node.acq_addr)
            .unwrap();
        app_data.remove(pos);
        debug!("Removing node {} from global config", &node.acq_addr);
        self.global_node_config.remove_node_info(node)
    }

    pub fn remove_node_info(&mut self, app: &str, node: &NodeInfo) -> Result<()> {
        if self.should_remove_node_info(app, node) {
            self.remove_node_info_checked(app, node)?;
        }

        Ok(())
    }

    pub fn clear_app(&mut self, app: &str) -> Result<()> {
        if let Some(app_data) = self.node_data.remove(app) {
            for node in app_data {
                self.global_node_config.remove_node_info(&node)?;
            }
        }

        Ok(())
    }

    pub fn clear_all_app(&mut self) {
        self.node_data.clear();
        self.global_node_config.global_data.clear();
    }

    pub fn get_node_info(&self, app: &str, index: usize) -> Option<Arc<NodeInfo>> {
        self.node_data
            .get(app)
            .and_then(|app_data| app_data.get(index).cloned())
    }

    pub fn get_node_info_by_addr(&self, app: &str, acq_addr: &str) -> Option<Arc<NodeInfo>> {
        self.node_data
            .get(app)
            .and_then(|app_data| app_data.iter().find(|n| n.acq_addr == acq_addr))
            .cloned()
    }

    pub fn get_node_infos(&self, app: &str, index: usize, count: usize) -> Vec<Arc<NodeInfo>> {
        self.node_data
            .get(app)
            .map(|app_data| {
                app_data[index.min(app_data.len())..]
                    .iter()
                    .take(count)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn get_all_node_infos(&self, app: &str) -> Vec<Arc<NodeInfo>> {
        self.node_data
            .get(app)
            .map(|app_data| app_data.clone())
            .unwrap_or_default()
    }

    pub fn get_node_count(&self, app: &str) -> usize {
        self.node_data
            .get(app)
            .map(|app_data| app_data.len())
            .unwrap_or(0)
    }

    pub fn add_node_infos(&mut self, app: &str, nodes: Vec<NodeInfo>) -> Result<()> {
        for node in nodes {
            self.add_node_info(app, node)?;
        }
        Ok(())
    }

    pub fn add_node_infos_checked(&mut self, app: &str, nodes: Vec<NodeInfo>) {
        for node in nodes {
            self.add_node_info_checked(app, node);
        }
    }

    pub fn remove_node_infos(&mut self, app: &str, nodes: &[NodeInfo]) -> Result<()> {
        for node in nodes {
            self.remove_node_info(app, node)?;
        }
        Ok(())
    }

    pub fn remove_node_infos_checked(&mut self, app: &str, nodes: &[NodeInfo]) -> Result<()> {
        for node in nodes {
            self.remove_node_info_checked(app, node)?;
        }
        Ok(())
    }

    pub fn load_config(&mut self, config_path: Option<&Path>) -> Result<()> {
        self.node_data.clear();

        if let Some(path) = config_path {
            self.load_config_from_file(path)
        } else {
            self.load_config_from_db(get_db_path().as_path())
        }
    }

    pub fn save_config_to_file(&self, config_path: &Path) -> Result<()> {
        let mut result = serde_json::Map::new();

        for (app, nodes) in &self.node_data {
            let json_nodes: Vec<serde_json::Value> = nodes
                .iter()
                .map(|node| serde_json::to_value(node.as_ref()).unwrap())
                .collect();
            result.insert(app.clone(), serde_json::Value::Array(json_nodes));
        }

        let json = serde_json::Value::Object(result);

        std::fs::write(config_path, serde_json::to_string_pretty(&json).unwrap())
            .map_err(|e| anyhow::anyhow!("Failed to write config file: {}", e))
    }

    fn load_config_from_file(&mut self, config_path: &Path) -> Result<()> {
        // Implementation for loading from file
        unimplemented!()
    }

    fn load_config_from_db(&mut self, db_path: &Path) -> Result<()> {
        // Implementation for loading from database
        unimplemented!()
    }

    // ... Other methods ...
}

fn get_db_path() -> PathBuf {
    // Implementation to get the database path
    unimplemented!()
}
