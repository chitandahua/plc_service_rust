use serde_json::{json, Value};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use tracing::{error, info};

use crate::mqtt_handler::MqttTopicType;
use crate::protocol::app_data::{AddressSetRequest, ConfirmResponse};
use crate::protocol::Frame;
use crate::request_info;
use crate::{
    mqtt_message::{MqttMessage, MqttPayload},
    mqtt_topic::MqttTopic,
    protocol::app_data::Address,
    MqttMsgHandler, ReqInfo, Result, UartMessage, UartMsgHandler, APP_NAME,
};

use super::{IntoMqttMessage, UartResponse};

pub struct MasterAddress {
    node_addr: NodeAddress,
}

struct NodeAddress {
    address: Mutex<Address>,
}

const MASTER_NODE: &str = "masterNode";

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
                address: Mutex::new(Address::from(esn.as_str())),
            },
        }
    }

    pub fn mqtt_get_address(
        &self,
        message: MqttMessage,
        mqtt_msg_sender: &mpsc::Sender<MqttMessage>,
        uart_msg_sender: &mpsc::Sender<UartMessage>,
    ) -> Result<()> {
        let address = {
            let address = self.node_addr.address.lock().unwrap();
            address.to_string()
        };

        let response = json!(
            {
                MASTER_NODE: address
            }
        );

        mqtt_msg_sender.send(MqttMessage::new_with_msg_body(message, Some(response)))?;

        Ok(())
    }

    pub fn mqtt_set_address(
        message: MqttMessage,
        uart_msg_sender: &mpsc::Sender<UartMessage>,
    ) -> Result<()> {
        let payload: Value = serde_json::from_str(message.payload()).unwrap();
        let address = Address::from(payload[MASTER_NODE].as_str().unwrap());
        let address_clone = address.clone();

        let request = AddressSetRequest::new(address);
        let frame = Frame::new_request(request);
        let req_info = ReqInfo::new_with_mqtt(
            &frame,
            message.topic(),
            payload["token"].as_str().unwrap(),
            Some(Box::new(address_clone)),
            Some(Arc::new(request_info::timeout_handler)),
        );
        uart_msg_sender.send(UartMessage::new(req_info, frame))?;

        Ok(())
    }

    pub fn uart_set_address(
        &self,
        message: UartMessage,
        mqtt_msg_sender: &mpsc::Sender<MqttMessage>,
    ) -> Result<()> {
        let response = UartResponse::<ConfirmResponse>::try_from(message.frame)?;
        let is_init = message.req_info.is_init();

        match is_init {
            true => {} // TODO
            false => {
                let mut mqtt_req_info = message.req_info.into_mqtt_req_info().unwrap();
                let message = match response {
                    UartResponse::Normal(response) => {
                        {
                            let mut address = self.node_addr.address.lock().unwrap();
                            *address = *mqtt_req_info
                                .extra_data()
                                .unwrap()
                                .downcast::<Address>()
                                .unwrap();
                        }
                        response.into_mqtt_message(mqtt_req_info)
                    }
                    UartResponse::Deny(response) => response.into_mqtt_message(mqtt_req_info),
                };
                mqtt_msg_sender.send(message)?;
            }
        }

        Ok(())
    }

    pub fn init(&self, mqtt_msg_handler: &mut MqttMsgHandler) {
        const ADDRESS_OBJECT: &str = "/masterNode";
        let get_address_topic = format!("{}{}{}", "+/get/request/", APP_NAME, ADDRESS_OBJECT);
        mqtt_msg_handler.add_topic_filter(get_address_topic, MqttTopicType::GetMasterAddress);

        let set_address_topic = format!("{}{}{}", "+/set/request/", APP_NAME, ADDRESS_OBJECT);
        mqtt_msg_handler.add_topic_filter(set_address_topic, MqttTopicType::SetMasterAddress);
    }
}
