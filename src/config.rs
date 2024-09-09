use serde::Deserialize;

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
