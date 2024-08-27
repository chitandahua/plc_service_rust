use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::{
    path::PathBuf,
    sync::{mpsc, Arc},
};

mod cli;
pub use cli::Args;

mod mqtt_agent;
use mqtt_agent::{MqttClient, MqttHandler};

mod mqtt_message;
use mqtt_message::{MqttMessage, MqttPayload};

mod mqtt_topic;
use mqtt_topic::{MqttTopic, TopicError};

mod mqtt_handler;
use mqtt_handler::{CallBack, Handler, MqttMsgHandler};

mod protocol;

mod request_info;
use request_info::{ReqInfo, UartMessage};

mod serial_port;
use serial_port::UartPort;

mod uart_agent;
use uart_agent::{UartAgent, UartHandler};

mod service;
use service::{MasterAddress, ModuleInfo};

mod uart_handler;
use uart_handler::UartMsgHandler;

pub type Error = anyhow::Error;

pub type Result<T> = anyhow::Result<T, Error>;

pub const APP_NAME: &str = "PLCServiceGW";

const MQTT_CONFIG_PATH: &str = "./mqtt_server.json";
const UART_CONFIG_PATH: &str = "./com_setting.json";

pub fn run(args: Args) -> Result<()> {
    tracing::debug!("mqtt client start");

    let mqtt_client = MqttClient::from_file(MQTT_CONFIG_PATH.into())?;

    // mqtt
    let (uart_msg_sender, uart_msg_receiver) = mpsc::channel();
    let (sender, receiver) = mpsc::channel();
    let (msg_sender, msg_receiver) = mpsc::channel();

    //module_init(&sender, &uart_msg_sender);
    let module_info_cb = ModuleInfo::callback(&sender, &uart_msg_sender);
    let master_address = MasterAddress::new("123456789012".to_string());
    let master_address_cb = master_address.callback(&sender, &uart_msg_sender);

    let mut mqtt_msg_handler = MqttMsgHandler::new(sender.clone(), msg_receiver);

    mqtt_msg_handler.register_req_callback(module_info_cb.0, module_info_cb.1);
    mqtt_msg_handler.register_req_callback(master_address_cb.0, master_address_cb.1);

    // uart
    let mut uart_handler = UartMsgHandler::new(sender.clone(), uart_msg_sender.clone());
    let uart_agent = UartAgent::new(uart_msg_receiver);

    let handler = Handler::new(msg_sender, mqtt_msg_handler.subscribe_topics());

    let mut join_handler = mqtt_client.run(handler, receiver)?;
    let sock_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 34567);
    join_handler.extend(uart_agent.run(
        PathBuf::from(UART_CONFIG_PATH),
        Some(sock_addr),
        uart_handler,
    )?);
    join_handler.extend(mqtt_msg_handler.run()?);

    for handler in join_handler {
        handler.join().unwrap();
    }
    println!("shutting down!");

    Ok(())
}

fn module_init(
    mqtt_msg_sender: &mpsc::Sender<MqttMessage>,
    uart_msg_sender: &mpsc::Sender<UartMessage>,
) {
    //ModuleInfo::init(mqtt_msg_sender, mqtt_msg_sender);
    //let master_address = MasterAddress::new("123456789012".to_string());
    //master_address.init(mqtt_msg_sender, uart_msg_sender);
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
