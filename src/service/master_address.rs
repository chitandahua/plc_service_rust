use serde_json::{json, Value};
use std::sync::{mpsc, Mutex};

use crate::mqtt_handler::MqttTopicType;
use crate::mqtt_message::{MqttMessage, PayloadBody};
use crate::protocol::app_data::{AddressSetRequest, ConfirmResponse};
use crate::protocol::Frame;
use crate::request_info::MqttReqInfo;
use crate::service::{IntoMqttMessage, UartResponse};
use crate::{protocol::app_data::Address, MqttMsgHandler, ReqInfo, Result, UartMessage, APP_NAME};

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

    pub fn get_master_address(&self) -> Address {
        let address = self.node_addr.address.lock().unwrap();
        address.to_owned()
    }

    pub fn mqtt_get_address(
        &self,
        message: MqttMessage,
        mqtt_msg_sender: &mpsc::Sender<MqttMessage>,
    ) -> Result<()> {
        let address = self.get_master_address().to_string();

        let response = json!(
            {
                MASTER_NODE: address
            }
        );

        mqtt_msg_sender.send(MqttMessage::new_with_msg_body(
            message,
            Some(PayloadBody::Flat(response)),
        ))?;

        Ok(())
    }

    fn set_address(
        address: Address,
        mqtt_req_info: Option<MqttReqInfo>,
        uart_msg_sender: &mpsc::Sender<UartMessage>,
    ) {
        let request = AddressSetRequest::new(address);
        let frame = Frame::new_request(None, request);
        let req_info = ReqInfo::new(&frame, mqtt_req_info);
        uart_msg_sender
            .send(UartMessage::new(req_info, frame))
            .unwrap();
    }

    pub fn mqtt_set_address(
        message: MqttMessage,
        uart_msg_sender: &mpsc::Sender<UartMessage>,
    ) -> Result<()> {
        let payload: Value = serde_json::from_str(message.payload()).unwrap();
        let address = Address::from(payload[MASTER_NODE].as_str().unwrap());
        let mqtt_req_info = MqttReqInfo::new(
            message.topic(),
            payload["token"].as_str().unwrap(),
            Some(Box::new(address.clone())),
        );
        Self::set_address(address, Some(mqtt_req_info), uart_msg_sender);

        Ok(())
    }

    pub fn init_set_address(
        &self,
        master_address: Address,
        uart_msg_sender: &mpsc::Sender<UartMessage>,
    ) {
        Self::set_address(master_address, None, uart_msg_sender);
    }

    pub fn init_set_address_response(&self, message: UartMessage) -> Result<()> {
        UartResponse::<ConfirmResponse>::try_from(message.frame)?.into()
    }

    pub fn uart_set_address(
        &self,
        message: UartMessage,
        mqtt_msg_sender: &mpsc::Sender<MqttMessage>,
    ) -> Result<()> {
        let response = UartResponse::<ConfirmResponse>::try_from(message.frame)?;
        let mut mqtt_req_info = message.req_info.into_mqtt_req_info().unwrap();
        let message = match response {
            UartResponse::Normal(response) => {
                {
                    let mut address = self.node_addr.address.lock().unwrap();
                    *address = *mqtt_req_info
                        .get_extra_data()
                        .unwrap()
                        .downcast::<Address>()
                        .unwrap();
                }
                response.into_mqtt_message(mqtt_req_info)
            }
            UartResponse::Deny(response) => response.into_mqtt_message(mqtt_req_info),
        };
        mqtt_msg_sender.send(message)?;

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
