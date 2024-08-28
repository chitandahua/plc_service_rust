use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::rc::{Rc, Weak};
use thiserror::Error;

use crate::Result;

#[derive(Debug, PartialEq, Error)]
enum NodeConfigError {
    #[error("acq addr {0} not found")]
    NotFound(String),
    #[error("node info type mismatch, expected {0}, got {1}")]
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

    pub fn add_node_info(&mut self, node: NodeInfo) -> Rc<NodeInfo> {
        let arc_node = Rc::new(node);
        self.global_data
            .insert(arc_node.acq_addr.clone(), Rc::downgrade(&arc_node));
        arc_node
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

    pub fn get_node_info(&self, acq_addr: &str) -> Option<Rc<NodeInfo>> {
        self.global_data.get(acq_addr).and_then(Weak::upgrade)
    }

    pub fn get_all_node_infos(&self) -> Vec<Rc<NodeInfo>> {
        self.global_data
            .values()
            .filter_map(Weak::upgrade)
            .collect()
    }
}

pub(crate) struct NodeConfig {
    node_data: HashMap<String, Vec<Rc<NodeInfo>>>,
    global_node_config: GlobalNodeConfig,
}

impl NodeConfig {
    pub fn new() -> Self {
        NodeConfig {
            node_data: HashMap::new(),
            global_node_config: GlobalNodeConfig::new(),
        }
    }

    pub fn add_node_info(&mut self, app: &str, node: NodeInfo) -> Result<()> {
        let app_data = self
            .node_data
            .entry(app.to_string())
            .or_insert_with(Vec::new);

        if let Some(existing) = app_data.iter().find(|n| n.acq_addr == node.acq_addr) {
            //existing.pro_type = node.pro_type.clone();
            //type不同不允许覆盖
            anyhow::ensure!(
                existing.pro_type == node.pro_type,
                NodeConfigError::TypeMismatch(node.pro_type, existing.pro_type.clone())
            );
        } else {
            let arc_node = self.global_node_config.add_node_info(node);
            app_data.push(arc_node);
        }

        Ok(())
    }

    pub fn remove_node_info(&mut self, app: &str, node: &NodeInfo) -> Result<()> {
        if let Some(app_data) = self.node_data.get_mut(app) {
            if let Some(pos) = app_data.iter().position(|n| n.acq_addr == node.acq_addr) {
                app_data.remove(pos);
                self.global_node_config.remove_node_info(node)
            } else {
                anyhow::bail!(NodeConfigError::NotFound(node.acq_addr.clone()))
            }
        } else {
            anyhow::bail!(NodeConfigError::NotFound(node.acq_addr.clone()))
        }
    }

    pub fn clear_app(&mut self, app: &str) -> Result<()> {
        if let Some(app_data) = self.node_data.remove(app) {
            for node in app_data {
                self.global_node_config.remove_node_info(&node)?;
            }
        }

        Ok(())
    }

    pub fn get_node_info(&self, app: &str, index: usize) -> Option<Rc<NodeInfo>> {
        self.node_data
            .get(app)
            .and_then(|app_data| app_data.get(index).cloned())
    }

    pub fn get_node_info_by_addr(&self, app: &str, acq_addr: &str) -> Option<Rc<NodeInfo>> {
        self.node_data
            .get(app)
            .and_then(|app_data| app_data.iter().find(|n| n.acq_addr == acq_addr))
            .cloned()
    }

    pub fn get_node_infos(&self, app: &str, index: usize, count: usize) -> Vec<Rc<NodeInfo>> {
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

    pub fn get_all_node_infos(&self, app: &str) -> Vec<Rc<NodeInfo>> {
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

    pub fn remove_node_infos(&mut self, app: &str, nodes: &[NodeInfo]) -> Result<()> {
        for node in nodes {
            self.remove_node_info(app, node)?;
        }
        Ok(())
    }

    pub fn load_config(&mut self, config_path: Option<&str>) -> Result<()> {
        self.node_data.clear();

        if let Some(path) = config_path {
            self.load_config_from_file(path)
        } else {
            self.load_config_from_db(&get_db_path())
        }
    }

    pub fn save_config_to_file(&self, config_path: &str) -> Result<()> {
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

    fn load_config_from_file(&mut self, config_path: &str) -> Result<()> {
        // Implementation for loading from file
        unimplemented!()
    }

    fn load_config_from_db(&mut self, db_path: &str) -> Result<()> {
        // Implementation for loading from database
        unimplemented!()
    }

    // ... Other methods ...
}

fn get_db_path() -> String {
    // Implementation to get the database path
    unimplemented!()
}
