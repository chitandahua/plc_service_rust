use serde_json::json;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};

use crate::{
    mqtt_message::{MqttMessage, MqttPayload},
    mqtt_topic::MqttTopic,
    protocol::app_data::Address,
    CallBack, MqttMsgHandler, Result, UartMessage, UartMsgHandler, APP_NAME,
};

pub struct MasterAddress {
    node_addr: NodeAddress,
}

#[derive(Clone)]
pub struct NodeAddress {
    address: Arc<Mutex<Address>>,
}

impl MasterAddress {
    pub fn new(esn: String) -> Self {
        // esn中可能有字母需去除 取12个数字 不足则前面补0
        let esn = esn
            .chars()
            .filter(|c| c.is_ascii_digit())
            .take(12)
            .collect::<String>();

        MasterAddress {
            node_addr: NodeAddress {
                address: Arc::new(Mutex::new(Address::from(esn.as_str()))),
            },
        }
    }

    pub fn node_addr(&self) -> NodeAddress {
        self.node_addr.clone()
    }

    pub fn callback<'a>(
        &'a self,
        mqtt_msg_sender: &'a mpsc::Sender<MqttMessage>,
        uart_msg_sender: &'a mpsc::Sender<UartMessage>,
    ) -> (String, CallBack<'a>) {
        let get_addr_callback = |msg| {
            let mqtt_msg_sender = mqtt_msg_sender.clone();
            let uart_msg_sender = uart_msg_sender.clone();
            let node_addr = self.node_addr();
            mqtt_get_address(msg, node_addr, mqtt_msg_sender, uart_msg_sender)
        };

        let get_addr_topic = format!("{}{}{}", "+/get/request/", APP_NAME, "/masterNode");
        (get_addr_topic, Arc::new(get_addr_callback))
    }
}

pub fn mqtt_get_address(
    message: MqttMessage,
    node_addr: NodeAddress,
    mqtt_msg_sender: mpsc::Sender<MqttMessage>,
    uart_msg_sender: mpsc::Sender<UartMessage>,
) -> Result<()> {
    let address = {
        let address = node_addr.address.lock().unwrap();
        address.clone()
    };

    let response = json!(
        {
            "masterNode": address.to_string()
        }
    );

    let payload = MqttPayload::new_with_token(message.get_token(), response);
    let topic: MqttTopic = message.topic().try_into().unwrap();
    mqtt_msg_sender.send(MqttMessage::new(
        topic.topic_transfer(),
        payload.to_string(),
    ))?;

    Ok(())
}
