use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Weak};
use thiserror::Error;
use tracing::{debug, error, info};

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

    pub fn _size(&self) -> usize {
        self.global_data.len()
    }

    pub fn get_node_info(&self, acq_addr: &str) -> Option<Arc<NodeInfo>> {
        self.global_data.get(acq_addr).and_then(Weak::upgrade)
    }

    pub fn _get_all_node_infos(&self) -> Vec<Arc<NodeInfo>> {
        self.global_data
            .values()
            .filter_map(Weak::upgrade)
            .collect()
    }
}

pub(crate) struct NodeConfig {
    node_data: HashMap<String, Vec<Arc<NodeInfo>>>,
    global_node_config: GlobalNodeConfig,
    db_conn: Option<Connection>,
    config_path: Option<PathBuf>,
}

impl NodeConfig {
    pub fn new(config_path: Option<PathBuf>) -> Result<Self> {
        let db_conn = config_path
            .as_ref()
            .map_or(Some(Connection::open(Self::get_db_path())?), |_| None);
        Ok(NodeConfig {
            node_data: HashMap::new(),
            global_node_config: GlobalNodeConfig::new(),
            db_conn,
            config_path,
        })
    }

    pub fn add_node_info_exist(
        &mut self,
        app: &str,
        node: &NodeInfo,
        is_init: bool,
    ) -> Result<bool> {
        let app_data = self.node_data.entry(app.to_string()).or_default();

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
            app_data.push(existing);
            if !is_init {
                self.add_config(app, vec![node.clone()].as_ref())?;
            }
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
        self.node_data
            .entry(app.to_string())
            .or_default()
            .push(node);
    }

    // 当前仅初始化时调用
    fn add_node_info(&mut self, app: &str, node: NodeInfo) -> Result<()> {
        if !(self.add_node_info_exist(app, &node, true)?) {
            self.add_node_info_checked(app, node);
        }

        Ok(())
    }

    pub fn should_remove_node_info(&mut self, app: &str, node: &NodeInfo) -> Result<bool> {
        let app_data = match self.node_data.get_mut(app) {
            Some(data) => data,
            None => return Ok(false),
        };

        let pos = match app_data.iter().position(|n| n.acq_addr == node.acq_addr) {
            Some(p) => p,
            None => return Ok(false),
        };

        debug!(
            "Attempting to remove node {} for app {}",
            &node.acq_addr, app
        );

        let count = Arc::strong_count(&app_data[pos]);
        if count == 1 {
            // 只有自己引用
            Ok(true)
        } else {
            // 如果还有其他引用，只从当前 app_data 中移除
            debug!(
                "Node {} still exists in other app(s), just removing from current app",
                &node.acq_addr
            );
            let node_info = app_data.remove(pos);
            self.del_config(app, vec![node_info.deref().clone()].as_ref())?;
            Ok(false)
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

    pub fn _remove_node_info(&mut self, app: &str, node: &NodeInfo) -> Result<()> {
        if self.should_remove_node_info(app, node)? {
            self.remove_node_info_checked(app, node)?;
        }

        Ok(())
    }

    pub fn _clear_app(&mut self, app: &str) -> Result<()> {
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

    pub fn _get_node_info(&self, app: &str, index: usize) -> Option<Arc<NodeInfo>> {
        self.node_data
            .get(app)
            .and_then(|app_data| app_data.get(index).cloned())
    }

    pub fn _get_node_info_by_addr(&self, app: &str, acq_addr: &str) -> Option<Arc<NodeInfo>> {
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
        self.node_data.get(app).cloned().unwrap_or_default()
    }

    pub fn _get_all_app_node_infos(&self) -> &HashMap<String, Vec<Arc<NodeInfo>>> {
        &self.node_data
    }

    pub fn get_node_count(&self, app: &str) -> usize {
        self.node_data
            .get(app)
            .map(|app_data| app_data.len())
            .unwrap_or(0)
    }

    fn add_node_infos(&mut self, app: &str, nodes: &[NodeInfo]) -> Result<()> {
        for node in nodes {
            self.add_node_info(app, node.to_owned())?;
        }
        Ok(())
    }

    pub fn add_node_infos_checked(&mut self, app: &str, nodes: &[NodeInfo]) -> Result<()> {
        for node in nodes {
            self.add_node_info_checked(app, node.to_owned());
        }
        self.add_config(app, nodes)
    }

    pub fn _remove_node_infos(&mut self, app: &str, nodes: &[NodeInfo]) -> Result<()> {
        for node in nodes {
            self._remove_node_info(app, node)?;
        }
        Ok(())
    }

    pub fn remove_node_infos_checked(&mut self, app: &str, nodes: &[NodeInfo]) -> Result<()> {
        for node in nodes {
            self.remove_node_info_checked(app, node)?;
        }
        self.del_config(app, nodes)
    }

    pub fn load_config(&mut self) -> Result<HashMap<String, Vec<NodeInfo>>> {
        if self.config_path.is_some() {
            self.load_config_from_file()
        } else if self.db_conn.is_some() {
            self.load_config_from_db()
        } else {
            unreachable!("Both config_path and db_conn are None")
        }
    }

    pub fn _save_config(&self) -> Result<()> {
        if self.config_path.is_some() {
            self.save_config_to_file(self.config_path.as_ref().unwrap())
        } else if self.db_conn.is_some() {
            self._save_config_to_db()
        } else {
            unreachable!("Both config_path and db_conn are None")
        }
    }

    pub fn add_config(&self, app: &str, node: &[NodeInfo]) -> Result<()> {
        if let Some(config_path) = &self.config_path {
            self.save_config_to_file(config_path.as_path())
        } else if self.db_conn.is_some() {
            self.add_db_config(app, node)
        } else {
            unreachable!("Both config_path and db_conn are None")
        }
    }

    pub fn del_config(&mut self, app: &str, nodes: &[NodeInfo]) -> Result<()> {
        if let Some(config_path) = &self.config_path {
            self.save_config_to_file(config_path.as_path())
        } else if self.db_conn.is_some() {
            self.del_db_config(app, nodes)
        } else {
            unreachable!("Both config_path and db_conn are None")
        }
    }
}

impl NodeConfig {
    fn load_config_from_file(&mut self) -> Result<HashMap<String, Vec<NodeInfo>>> {
        let mut result = HashMap::new();
        let config_path = self.config_path.as_ref().unwrap().to_owned();
        let value = serde_json::from_reader(std::fs::File::open(config_path)?)?;
        if let serde_json::Value::Object(map) = value {
            for (app, nodes) in map {
                if let serde_json::Value::Array(nodes) = nodes {
                    let nodes: std::result::Result<Vec<NodeInfo>, serde_json::Error> =
                        nodes.into_iter().map(serde_json::from_value).collect();
                    let nodes = nodes?;
                    self.add_node_infos(&app, &nodes)?;
                    result.insert(app, nodes);
                } else {
                    anyhow::bail!(
                        "Invalid config file: {}",
                        self.config_path.as_ref().unwrap().display()
                    );
                }
            }
        } else {
            anyhow::bail!(
                "Invalid config file: {}",
                self.config_path.as_ref().unwrap().display()
            );
        }
        Ok(result)
    }

    fn save_config_to_file(&self, config_path: &Path) -> Result<()> {
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
}

const SQL_TABLE_PREFIX: &str = "plc_device_";

impl NodeConfig {
    fn get_db_path() -> PathBuf {
        use crate::config::APP_PATH;
        APP_PATH.join("plc_config.db")
    }

    fn get_sql_table_name(app: &str) -> String {
        format!("{}{}", SQL_TABLE_PREFIX, app)
    }

    fn create_sqlite_table(&self, app: &str) -> Result<()> {
        let sql = format!(
            "CREATE TABLE IF NOT EXISTS {} (id INTEGER PRIMARY KEY AUTOINCREMENT, address TEXT, protocol TEXT)",
            Self::get_sql_table_name(app)
        );
        self.db_conn.as_ref().unwrap().execute(&sql, [])?;
        Ok(())
    }

    fn _drop_sqlite_table(&self, app: &str) -> Result<()> {
        let sql = format!("DROP TABLE IF EXISTS {}", Self::get_sql_table_name(app));
        self.db_conn.as_ref().unwrap().execute(&sql, [])?;
        Ok(())
    }

    fn add_sqlite_config(&self, app: &str, node: &NodeInfo) -> Result<()> {
        let sql = format!(
            "INSERT INTO {} (address, protocol) VALUES (?, ?)",
            Self::get_sql_table_name(app)
        );
        self.db_conn
            .as_ref()
            .unwrap()
            .execute(&sql, params![node.acq_addr, node.pro_type])?;
        Ok(())
    }

    fn del_sqlite_config(&self, app: &str, node: &NodeInfo) -> Result<()> {
        let sql = format!(
            "DELETE FROM {} WHERE address = ? AND protocol = ?",
            Self::get_sql_table_name(app)
        );
        self.db_conn
            .as_ref()
            .unwrap()
            .execute(&sql, params![node.acq_addr, node.pro_type])?;
        Ok(())
    }

    fn load_config_from_db(&mut self) -> Result<HashMap<String, Vec<NodeInfo>>> {
        let mut result = HashMap::new();
        let table_names = {
            let db_conn = self.db_conn.as_ref().unwrap();
            let mut stmt = db_conn
                .prepare("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE ?")?;
            let table_names: Vec<String> = stmt
                .query_map([format!("{}%", SQL_TABLE_PREFIX)], |row| row.get(0))?
                .filter_map(|result| result.ok())
                .collect();
            table_names
        };

        for table in table_names {
            if table.is_empty() {
                continue;
            }

            let app = &table[SQL_TABLE_PREFIX.len()..];
            let nodes: Vec<NodeInfo> = {
                let sql = format!("SELECT address, protocol FROM {}", table);
                let mut stmt = self.db_conn.as_ref().unwrap().prepare(&sql)?;
                let nodes = stmt.query_map([], |row| {
                    Ok(NodeInfo {
                        acq_addr: row.get(0)?,
                        pro_type: row.get(1)?,
                    })
                })?;
                nodes.filter_map(|result| result.ok()).collect()
            };

            debug!("Loading config from db: app={}, nodes={:?}", app, nodes);

            self.add_node_infos(app, &nodes)?;
            result.insert(app.to_owned(), nodes);
        }

        Ok(result)
    }

    fn _save_config_to_db(&self) -> Result<()> {
        for (app, nodes) in &self.node_data {
            self._drop_sqlite_table(app)?;
            self.create_sqlite_table(app)?;

            for node in nodes {
                self.add_sqlite_config(app, node)?;
            }
        }

        Ok(())
    }

    fn add_db_config(&self, app: &str, nodes: &[NodeInfo]) -> Result<()> {
        self.create_sqlite_table(app)?;

        for node in nodes {
            self.add_sqlite_config(app, node)?;
        }

        Ok(())
    }

    fn del_db_config(&self, app: &str, nodes: &[NodeInfo]) -> Result<()> {
        for node in nodes {
            self.del_sqlite_config(app, node)?;
        }

        Ok(())
    }
}
