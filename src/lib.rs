mod cli;
pub use cli::Args;

mod mqtt_agent;
use mqtt_agent::MqttClient;
use mqtt_agent::MqttHandler;

mod mqtt_message;
use mqtt_message::{MqttMessage, MqttPayload};

mod mqtt_topic;
use mqtt_topic::{MqttTopic, TopicError};

mod mqtt_handler;
use mqtt_handler::Handler;

pub type Error = anyhow::Error;

pub type Result<T> = anyhow::Result<T, Error>;

pub const APP_NAME: &str = "PLCService";

const MQTT_CONFIG_PATH: &str = "./mqtt_server.json";

pub fn run(args: Args) -> Result<()> {
    tracing::debug!("mqtt client start");

    let mqtt_client = MqttClient::from_file(MQTT_CONFIG_PATH.into())?;
    let handler = Handler::new(&mqtt_client);

    let join_handler = mqtt_client.run(handler)?;

    for handler in join_handler {
        handler.join().unwrap();
    }
    println!("shutting down!");

    Ok(())
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