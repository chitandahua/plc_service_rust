use anyhow::Context;
use std::sync::atomic::AtomicU8;
use std::{
    path::PathBuf,
    sync::{mpsc, Arc},
};
use timer::Timer;

mod cli;
pub use cli::Args;

mod config;
use config::{MeterConfig, PlcDeviceConfig};

mod mqtt_agent;
use mqtt_agent::{MqttClient, MqttHandler};

mod mqtt_message;
use mqtt_message::{MqttMessage, MqttPayload};

mod mqtt_topic;
use mqtt_topic::MqttTopic;

mod mqtt_handler;
use mqtt_handler::{Handler, MqttMsgHandler};

mod protocol;

mod request_info;
use request_info::{ReqInfo, UartMessage};

mod serial_port;
use serial_port::UartPort;

mod uart_agent;
use uart_agent::{UartAgent, UartHandler};

mod service;
use service::{
    ConcurrentMeter, DeviceInfo, MasterAddress, ModuleInfo, ModuleService, NodeManage, PlcDevice,
    PlcInit,
};

mod uart_handler;
use uart_handler::{UartMsgHandler, UartTimeoutHandler};

pub type Error = anyhow::Error;

pub type Result<T> = anyhow::Result<T, Error>;

pub const APP_NAME: &str = "PLCServiceGW";

const MQTT_CONFIG_PATH: &str = "./mqtt_server.json";
const UART_CONFIG_PATH: &str = "./com_setting.json";
const METER_CONFIG_PATH: &str = "./meter_config.json";
const PLC_DEVICE_PATH: &str = "./plc_device.json";

pub fn run(args: Args) -> Result<()> {
    let meter_config = get_meter_config(&args)?;

    tracing::debug!("app start");

    let mqtt_client = MqttClient::from_file(MQTT_CONFIG_PATH.into())?;

    // mqtt
    let (uart_msg_sender, uart_msg_receiver) = mpsc::channel();
    let (concurrent_msg_sender, concurrent_msg_receiver) = mpsc::channel();
    let (sender, receiver) = mpsc::channel();
    let (msg_sender, msg_receiver) = mpsc::channel();

    let mut mqtt_msg_handler = MqttMsgHandler::new(
        sender.clone(),
        uart_msg_sender.clone(),
        concurrent_msg_sender.clone(),
        msg_receiver,
    );

    // timer
    let timer = Arc::new(Timer::new());
    let services = module_init(&mut mqtt_msg_handler, timer.clone(), &meter_config);

    let plc_init = Arc::new(PlcInit::new(
        uart_msg_sender.clone(),
        services.clone(),
        meter_config.uart_timeout,
        meter_config.init_timeout,
    ));
    // uart
    let uart_timeout_handler = UartTimeoutHandler::new(
        sender.clone(),
        concurrent_msg_sender.clone(),
        services.clone(),
    );
    let uart_handler = UartMsgHandler::new(
        sender.clone(),
        uart_msg_sender.clone(),
        concurrent_msg_sender.clone(),
        services.clone(),
        plc_init.clone(),
    );
    let uart_agent = UartAgent::new(
        uart_msg_receiver,
        concurrent_msg_receiver,
        PathBuf::from(UART_CONFIG_PATH),
        args.tcp_addr,
        meter_config.uart_timeout,
        meter_config.concurrent.timeout,
    )?;

    let handler = Handler::new(msg_sender, mqtt_msg_handler.subscribe_topics());
    let consecutive_timeouts = Arc::new(AtomicU8::new(0));

    let mut join_handler = mqtt_client.run(handler, receiver)?;
    join_handler.extend(uart_agent.run(
        uart_handler,
        timer.clone(),
        uart_timeout_handler,
        consecutive_timeouts.clone(),
    )?);
    join_handler.extend(mqtt_msg_handler.run(services)?);

    let device_info = DeviceInfo::new();
    device_info.run(&sender)?;

    //plc_init.run()?;
    let plc_device_config: PlcDeviceConfig =
        serde_json::from_reader(std::fs::File::open(PLC_DEVICE_PATH)?)?;
    let plc_device = PlcDevice::new(
        plc_device_config.port.parse()?,
        sender.clone(),
        plc_init,
        consecutive_timeouts,
    );
    plc_device.run()?;

    for handler in join_handler {
        handler.join().unwrap();
    }
    tracing::info!("shutting down!");

    Ok(())
}

fn module_init(
    mqtt_msg_handler: &mut MqttMsgHandler,
    timer: Arc<Timer>,
    meter_config: &MeterConfig,
) -> ModuleService {
    ModuleInfo::init(mqtt_msg_handler);
    let master_address = MasterAddress::new("123456789012".to_string());
    master_address.init(mqtt_msg_handler);
    let node_manage = NodeManage::new(None, meter_config.uart_timeout as u64);
    node_manage.init(mqtt_msg_handler);

    let concurrent_meter = ConcurrentMeter::new(&timer, meter_config.meter_reading.clone());
    concurrent_meter.init(mqtt_msg_handler);

    ModuleService::new(master_address, node_manage, concurrent_meter)
}

fn get_meter_config(args: &Args) -> Result<MeterConfig> {
    let mut meter_config: MeterConfig = serde_json::from_reader(
        std::fs::File::open(METER_CONFIG_PATH)
            .with_context(|| format!("open {} failed", METER_CONFIG_PATH))?,
    )
    .with_context(|| format!("parse {} failed", METER_CONFIG_PATH))?;

    args.init_timeout
        .map(|init_timeout| meter_config.init_timeout = init_timeout);
    args.uart_timeout
        .map(|uart_timeout| meter_config.uart_timeout = uart_timeout);

    // concurrent
    args.concurrent_limit
        .concurrency
        .map(|concurrency| meter_config.concurrent.concurrency_limit = concurrency);
    args.concurrent_limit
        .timeout
        .map(|timeout| meter_config.concurrent.timeout = timeout);

    // meter
    args.meter_reading
        .aging_time
        .map(|aging_time| meter_config.meter_reading.queue_aging_time = aging_time);
    args.meter_reading
        .max_addr_num
        .map(|max_addr_num| meter_config.meter_reading.concurrent_addr = max_addr_num);
    args.meter_reading
        .queue_size
        .map(|queue_size| meter_config.meter_reading.cache_queue_size = queue_size);

    Ok(meter_config)
}

const APP_VERSION: &str = "ST01.000";
const COMPILE_TIME: time::Time = compile_time::time!();

pub fn get_version_info() {
    //println!(
    //    "{} {}",
    //    APP_VERSION,
    //    env!("VERGEN_BUILD_TIMESTAMP")
    //);

    let hour = COMPILE_TIME.hour() + 8;
    let minute = COMPILE_TIME.minute();
    let second = COMPILE_TIME.second();
    let time_string = format!("{hour:02}:{minute:02}:{second:02}");

    println!(
        "{} {} {}",
        APP_VERSION,
        compile_time::date_str!(),
        time_string
    );
}

#[derive(thiserror::Error, Debug)]
pub enum MqttResponseError {
    #[error("request timeout")]
    Timeout,
}
