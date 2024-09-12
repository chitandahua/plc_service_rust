use std::sync::atomic::AtomicU8;
use std::sync::{mpsc, Arc};
use timer::Timer;

mod cli;
pub use cli::Args;

mod config;
use config::{Config, MeterConfig, MqttConfig, PlcDeviceConfig, UartConfig};

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
//use service::{DeviceInfo, PlcDevice};
use service::{ModuleInfo, ModuleService, PlcInit};

mod uart_handler;
use uart_handler::{UartMsgHandler, UartTimeoutHandler};

pub type Error = anyhow::Error;

pub type Result<T> = anyhow::Result<T, Error>;

pub const APP_NAME: &str = "PLCServiceGW";

pub struct PlcService {
    meter_config: MeterConfig,
    plc_device_config: PlcDeviceConfig,
    mqtt_client: MqttClient,
    uart_agent: UartAgent,
    module_service: ModuleService,
    timer: Arc<Timer>,
}

impl PlcService {
    pub fn new(args: Args) -> Result<Self> {
        let mut config = Config::new()?;
        set_meter_config(&mut config.meter_config, &args);

        let mqtt_client = MqttClient::from_config(config.mqtt_config)?;
        let uart_agent = UartAgent::new(
            config.uart_config,
            args.tcp_addr,
            config.meter_config.uart_timeout,
            config.meter_config.concurrent.timeout,
        )?;
        let timer = Arc::new(Timer::new());
        let module_service = ModuleService::new(timer.clone(), &config.meter_config)?;

        Ok(Self {
            meter_config: config.meter_config,
            plc_device_config: config.plc_device_config,
            mqtt_client,
            uart_agent,
            module_service,
            timer,
        })
    }

    pub fn run(self) -> Result<()> {
        let (uart_msg_sender, uart_msg_receiver) = mpsc::channel();
        let (concurrent_msg_sender, concurrent_msg_receiver) = mpsc::channel();
        let (mqtt_msg_sender, mqtt_msg_receiver) = mpsc::channel();
        let (msg_sender, msg_receiver) = mpsc::channel();

        let mut mqtt_msg_handler = MqttMsgHandler::new(
            mqtt_msg_sender.clone(),
            uart_msg_sender.clone(),
            concurrent_msg_sender.clone(),
            msg_receiver,
        );
        self.module_service.init(&mut mqtt_msg_handler);

        let uart_timeout_handler = UartTimeoutHandler::new(
            mqtt_msg_sender.clone(),
            concurrent_msg_sender.clone(),
            self.module_service.clone(),
        );

        let plc_init = Arc::new(PlcInit::new(
            uart_msg_sender.clone(),
            self.module_service.clone(),
            self.meter_config.uart_timeout,
            self.meter_config.init_timeout,
        ));
        let uart_handler = UartMsgHandler::new(
            mqtt_msg_sender.clone(),
            uart_msg_sender.clone(),
            concurrent_msg_sender.clone(),
            self.module_service.clone(),
            plc_init.clone(),
        );

        let handler = Handler::new(msg_sender, mqtt_msg_handler.subscribe_topics());
        let consecutive_timeouts = Arc::new(AtomicU8::new(0));

        let mut join_handler = self.mqtt_client.run(handler, mqtt_msg_receiver)?;
        join_handler.extend(self.uart_agent.run(
            uart_msg_receiver,
            concurrent_msg_receiver,
            uart_handler,
            self.timer.clone(),
            uart_timeout_handler,
            consecutive_timeouts.clone(),
        )?);
        join_handler.extend(mqtt_msg_handler.run(self.module_service.clone())?);

        // 初始化流程
        //let device_info = DeviceInfo::new();
        //device_info.run(&mqtt_msg_sender)?;

        self.module_service
            .master_address
            //.update_address(device_info.esn());
            .update_address("123456789012".to_string());

        plc_init.run()?;
        //let plc_device = PlcDevice::new(
        //    self.plc_device_config.port.parse()?,
        //    mqtt_msg_sender.clone(),
        //    plc_init,
        //    consecutive_timeouts,
        //);
        //join_handler.push(plc_device.run()?);

        for handler in join_handler {
            handler.join().unwrap();
        }
        tracing::info!("shutting down!");

        Ok(())
    }
}

fn set_meter_config(meter_config: &mut MeterConfig, args: &Args) {
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
}

const APP_VERSION: &str = "ST01.000";
const COMPILE_TIME: time::Time = compile_time::time!();

pub fn get_version_info() {
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
