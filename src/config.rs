use crate::Result;
use anyhow::Context;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use std::fs::File;
use std::path::PathBuf;
use std::sync::LazyLock;

#[derive(Debug, Deserialize)]
pub struct ConcurrentConfig {
    pub concurrency_limit: usize,
    pub timeout: u32,
}

#[derive(Debug, Deserialize, Clone)]
pub struct MeterReadingConfig {
    pub cache_queue_size: usize,
    pub concurrent_addr: usize,
    pub queue_aging_time: u32,
}

#[derive(Debug, Deserialize)]
pub struct MeterConfig {
    pub init_timeout: u32,
    pub uart_timeout: u32,
    pub concurrent: ConcurrentConfig,
    pub meter_reading: MeterReadingConfig,
}

#[derive(Debug, Deserialize)]
pub struct PlcDeviceConfig {
    pub port: String,
}

#[derive(Debug, Deserialize)]
pub struct MqttConfig {
    #[serde(rename = "Username")]
    pub username: String,
    #[serde(rename = "Password")]
    pub password: String,
    #[serde(rename = "ServiceIp")]
    pub service_ip: String,
    #[serde(rename = "ServicePort")]
    pub service_port: u16,
    #[serde(rename = "ClientId")]
    pub client_id: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct UartConfig {
    pub port: String,
    #[serde(rename = "baudrate")]
    pub baud_rate: u32,
    #[serde(rename = "wordlength")]
    pub word_length: u8,
    pub parity: String,
    #[serde(rename = "stopbit")]
    pub stop_bit: u8,
}

fn read_config<T: DeserializeOwned>(config_path: &PathBuf) -> Result<T> {
    let config = File::open(config_path)
        .with_context(|| format!("Failed to open config file {}", config_path.display()))?;
    Ok(serde_json::from_reader(config)
        .with_context(|| format!("Failed to parse config file {}", config_path.display()))?)
}

pub struct Config {
    pub meter_config: MeterConfig,
    pub plc_device_config: PlcDeviceConfig,
    pub mqtt_config: MqttConfig,
    pub uart_config: UartConfig,
}

pub static APP_PATH: LazyLock<PathBuf> = LazyLock::new(|| {
    std::env::current_dir().expect("Failed to get current directory")
    //format!("/userdata/dgri/{}", APP_NAME).into()
});
//static PROJECT_PATH: LazyLock<PathBuf> = LazyLock::new(|| APP_PATH.join("project").to_path_buf());
static PROJECT_PATH: LazyLock<PathBuf> = LazyLock::new(|| APP_PATH.clone());
pub static SCHEMA_PATH: LazyLock<PathBuf> = LazyLock::new(|| APP_PATH.join("schema").to_path_buf());

static MQTT_CONFIG_PATH: LazyLock<PathBuf> =
    LazyLock::new(|| PROJECT_PATH.join("mqtt_server.json").to_path_buf());
static UART_CONFIG_PATH: LazyLock<PathBuf> =
    LazyLock::new(|| PROJECT_PATH.join("com_setting.json").to_path_buf());
static METER_CONFIG_PATH: LazyLock<PathBuf> =
    LazyLock::new(|| APP_PATH.join("meter_config.json").to_path_buf());
static PLC_CONFIG_PATH: LazyLock<PathBuf> =
    LazyLock::new(|| APP_PATH.join("plc_device.json").to_path_buf());

impl Config {
    pub fn new() -> Result<Config> {
        Ok(Config {
            meter_config: read_config(&METER_CONFIG_PATH)?,
            plc_device_config: read_config(&PLC_CONFIG_PATH)?,
            mqtt_config: read_config(&MQTT_CONFIG_PATH)?,
            uart_config: read_config(&UART_CONFIG_PATH)?,
        })
    }
}
