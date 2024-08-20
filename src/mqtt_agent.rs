use anyhow::Context;
use paho_mqtt::{Client, ConnectOptionsBuilder, CreateOptionsBuilder, Message};
use serde::Deserialize;
use std::fs::File;
use std::io::Read;
use std::path::PathBuf;
use std::sync::mpsc;
use std::thread::{self, sleep, JoinHandle};
use std::time::Duration;
use tracing::{debug, error, info, warn};

use paho_mqtt as mqtt;

use crate::{MqttMessage, Result, APP_NAME};

#[derive(Debug, Deserialize)]
struct MqttConfig {
    #[serde(rename = "Username")]
    username: String,
    #[serde(rename = "Password")]
    password: String,
    #[serde(rename = "ServiceIp")]
    service_ip: String,
    #[serde(rename = "ServicePort")]
    service_port: u16,
    #[serde(rename = "ClientId")]
    client_id: Option<String>,
}

pub trait MqttHandler {
    fn mqtt_msg_handler(&mut self, message: MqttMessage) -> Result<Option<MqttMessage>>;
    fn subscribe_topics(&self) -> Vec<String>;
}

pub struct MqttClient {
    client: Client,
    qos: i32,
    rx: mqtt::Receiver<Option<Message>>,
}

impl MqttClient {
    pub fn from_file(config_path: PathBuf) -> Result<Self> {
        let mut file = File::open(config_path).context("open mqtt config fail")?;
        let mut buffer = String::new();
        file.read_to_string(&mut buffer)
            .context("invalid mqtt config")?;

        let config: MqttConfig =
            serde_json::from_str(buffer.as_str()).context("invalid mqtt json config")?;

        debug!("mqtt server {}:{}", config.service_ip, config.service_port);
        let create_opts = CreateOptionsBuilder::new()
            .server_uri(format!(
                "tcp://{}:{}",
                config.service_ip, config.service_port
            ))
            .client_id(config.client_id.unwrap_or(APP_NAME.to_string()))
            .finalize();

        debug!(
            "mqtt connect options username:{} password:{}",
            config.username, config.password
        );
        let conn_opts = ConnectOptionsBuilder::new()
            .keep_alive_interval(Duration::from_secs(30))
            .clean_session(true)
            .user_name(config.username)
            .password(config.password)
            .finalize();

        let mut client = Client::new(create_opts).context("create mqtt client fail")?;
        client.set_timeout(Duration::from_secs(5));

        let rx = client.start_consuming();
        client.connect(conn_opts).with_context(|| {
            format!(
                "connect mqtt server {}:{} fail",
                config.service_ip, config.service_port,
            )
        })?;

        debug!("mqtt connected");
        Ok(Self {
            client,
            qos: mqtt::QOS_1,
            rx,
        })
    }

    pub fn run(
        self,
        handler: impl MqttHandler + Send + 'static,
        receiver: mpsc::Receiver<MqttMessage>,
    ) -> Result<Vec<JoinHandle<()>>> {
        debug!("mqtt client start");
        let MqttClient { client, qos, rx } = self;

        client
            .subscribe_many_same_qos(&handler.subscribe_topics(), qos)
            .context("subscribe topic fail")?;

        let client_clone = client.clone();
        let msg_send_thread = thread::spawn(move || {
            Self::publish(&client_clone, receiver, qos); // 接收publish message
        });
        let msg_recv_thread = thread::spawn(move || {
            Self::receive(&client, rx, qos, handler);
        });

        Ok(vec![msg_send_thread, msg_recv_thread])
    }

    fn receive(
        client: &Client,
        rx: mqtt::Receiver<Option<Message>>,
        qos: i32,
        mut handler: impl MqttHandler,
    ) {
        let mut rconn_attempt: usize = 0;

        for msg in rx.iter() {
            if let Some(msg) = msg {
                debug!("recv msg: {}", msg);
                let msg = MqttMessage::try_from(msg);
                match msg {
                    Ok(msg) => match handler.mqtt_msg_handler(msg) {
                        Ok(response) => {
                            if let Some(response) = response {
                                debug!("response: topic {}", response.topic());
                                debug!("payload {}", response.payload());
                                let response =
                                    mqtt::Message::new(response.topic(), response.payload(), qos);
                                Self::publish_msg(client, response);
                            }
                        }
                        Err(e) => error!(cause = ?e, "handle message error"),
                    },
                    Err(e) => error!(cause = ?e, "parse message error"),
                }
            } else if !client.is_connected() {
                warn!("Lost connection. Attempting reconnect...");
                while let Err(err) = client.reconnect() {
                    rconn_attempt += 1;
                    error!(cause = ?err, "Error reconnecting #{}", rconn_attempt);
                    sleep(Duration::from_secs(1));
                }
                info!("Reconnected.");
            }
        }
    }

    pub fn _subscribe(&self, topic: impl Into<String>) -> Result<()> {
        let topic = topic.into();
        debug!("subuscribe topic: {}", topic);
        self.client.subscribe(topic.as_str(), self.qos)?;
        Ok(())
    }

    fn publish(client: &Client, receiver: mpsc::Receiver<MqttMessage>, qos: i32) {
        while let Ok(msg) = receiver.recv() {
            let msg = mqtt::Message::new(msg.topic(), msg.payload(), qos);
            debug!("publish message: {}", msg);
            Self::publish_msg(client, msg);
        }
    }

    fn publish_msg(client: &Client, message: Message) {
        client
            .publish(message)
            .unwrap_or_else(|e| error!(cause = ?e, "publish error"));
    }

    pub fn client_id(&self) -> String {
        self.client.client_id()
    }
}
