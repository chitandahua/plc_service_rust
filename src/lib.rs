use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::{
    path::PathBuf,
    sync::{mpsc, Arc},
};
use timer::Timer;

mod cli;
pub use cli::Args;

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
    ConcurrentMeter, DeviceInfo, MasterAddress, ModuleInfo, ModuleService, NodeManage, PlcInit,
};

mod uart_handler;
use uart_handler::{UartMsgHandler, UartTimeoutHandler};

pub type Error = anyhow::Error;

pub type Result<T> = anyhow::Result<T, Error>;

pub const APP_NAME: &str = "PLCServiceGW";

const MQTT_CONFIG_PATH: &str = "./mqtt_server.json";
const UART_CONFIG_PATH: &str = "./com_setting.json";

pub fn run(_args: Args) -> Result<()> {
    tracing::debug!("mqtt client start");

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
    let services = module_init(&mut mqtt_msg_handler, timer.clone());

    let plc_init = Arc::new(PlcInit::new(
        uart_msg_sender.clone(),
        services.clone(),
        6000,
        1,
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
    let sock_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 34567);
    let uart_agent = UartAgent::new(
        uart_msg_receiver,
        concurrent_msg_receiver,
        PathBuf::from(UART_CONFIG_PATH),
        Some(sock_addr),
    )?;

    let handler = Handler::new(msg_sender, mqtt_msg_handler.subscribe_topics());

    let mut join_handler = mqtt_client.run(handler, receiver)?;
    join_handler.extend(uart_agent.run(uart_handler, timer.clone(), uart_timeout_handler)?);
    join_handler.extend(mqtt_msg_handler.run(services)?);

    let device_info = DeviceInfo::new();
    device_info.run(&sender)?;

    plc_init.run()?;

    for handler in join_handler {
        handler.join().unwrap();
    }
    println!("shutting down!");

    Ok(())
}

fn module_init(mqtt_msg_handler: &mut MqttMsgHandler, timer: Arc<Timer>) -> ModuleService {
    ModuleInfo::init(mqtt_msg_handler);
    let master_address = MasterAddress::new("123456789012".to_string());
    master_address.init(mqtt_msg_handler);
    let node_manage = NodeManage::new(None, 6);
    node_manage.init(mqtt_msg_handler);

    let concurrent_meter = ConcurrentMeter::new(&timer);
    concurrent_meter.init(mqtt_msg_handler);

    ModuleService::new(master_address, node_manage, concurrent_meter)
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
