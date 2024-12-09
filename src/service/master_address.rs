use serde::{Deserialize, Serialize};
use std::sync::{mpsc, Mutex};

use crate::mqtt_handler::MqttTopicType;
use crate::mqtt_message::{MqttMessage, PayloadBody};
use crate::protocol::app_data::{
    AddressSetRequest, ConfirmResponse, MasterAddressRequest, MasterAddressResponse,
};
use crate::request_info::MqttReqInfo;
use crate::service::parse_response::{
    mqtt_info_request_uart_handler, mqtt_request_uart_handler, uart_response_handler,
};
use crate::service::{IntoMqttMessage, UartResponse};
use crate::{protocol::app_data::Address, MqttMsgHandler, Result, UartMessage, APP_NAME};

pub struct MasterAddress {
    node_addr: NodeAddress,
}

struct NodeAddress {
    address: Mutex<Address>,
}

#[derive(Debug, Deserialize)]
struct MqttAddressSetRequest {
    #[serde(rename = "masterNode")]
    master_node: String,
}

impl From<MqttAddressSetRequest> for AddressSetRequest {
    fn from(req: MqttAddressSetRequest) -> Self {
        AddressSetRequest::new(Address::from(req.master_node.as_str()))
    }
}

#[derive(Debug, Serialize)]
struct MqttAddressGetResponse {
    #[serde(rename = "masterNode")]
    master_node: String,
}

impl From<MasterAddressResponse> for MqttAddressGetResponse {
    fn from(response: MasterAddressResponse) -> Self {
        MqttAddressGetResponse {
            master_node: response.master_addr.to_string(),
        }
    }
}

impl IntoMqttMessage for MqttAddressGetResponse {
    fn into_mqtt_message(self, mqtt_req_info: MqttReqInfo) -> MqttMessage {
        MqttMessage::new_with_req_info_body(
            mqtt_req_info,
            Some(PayloadBody::Flat(serde_json::to_value(self).unwrap())),
        )
    }
}

impl MasterAddress {
    pub fn new() -> Self {
        MasterAddress {
            node_addr: NodeAddress {
                address: Mutex::new(Address::default()),
            },
        }
    }

    pub fn update_address(&self, esn: String) {
        // esn中可能有字母需去除 取12个数字 不足则前面补0
        let addr = esn
            .chars()
            .filter(|c| c.is_ascii_digit())
            .take(12)
            .collect::<String>();
        let addr = format!("{:0>12}", addr);

        let mut address = self.node_addr.address.lock().unwrap();
        *address = Address::from(addr.as_str());
    }

    pub fn get_master_address(&self) -> Address {
        let address = self.node_addr.address.lock().unwrap();
        address.to_owned()
    }

    pub fn mqtt_get_address(
        &self,
        message: MqttMessage,
        uart_msg_sender: &mpsc::Sender<UartMessage>,
    ) -> Result<()> {
        mqtt_request_uart_handler::<MasterAddressRequest>(
            MasterAddressRequest,
            message,
            uart_msg_sender,
        );
        Ok(())
    }

    pub fn uart_get_address(
        message: UartMessage,
        mqtt_msg_sender: &mpsc::Sender<MqttMessage>,
    ) -> Result<()> {
        uart_response_handler::<MasterAddressResponse, MqttAddressGetResponse>(
            message,
            mqtt_msg_sender,
        )
    }

    pub fn mqtt_set_address(message: MqttMessage, uart_msg_sender: &mpsc::Sender<UartMessage>) {
        let request = serde_json::from_str::<MqttAddressSetRequest>(message.payload()).unwrap();
        let mqtt_req_info = MqttReqInfo::new(
            message.topic(),
            message.get_token(),
            Some(Box::new(Address::from(request.master_node.as_str()))),
        );
        mqtt_info_request_uart_handler::<AddressSetRequest>(
            AddressSetRequest::from(request),
            Some(mqtt_req_info),
            uart_msg_sender,
        );
    }

    pub fn init_set_address(
        &self,
        master_address: Address,
        uart_msg_sender: &mpsc::Sender<UartMessage>,
    ) {
        mqtt_info_request_uart_handler::<AddressSetRequest>(
            AddressSetRequest::new(master_address),
            None,
            uart_msg_sender,
        );
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
                ().into_mqtt_message(mqtt_req_info)
            }
            UartResponse::Deny(response) => response.into_mqtt_message(mqtt_req_info),
        };
        mqtt_msg_sender.send(message)?;

        Ok(())
    }

    pub fn init(&self, mqtt_msg_handler: &mut MqttMsgHandler) {
        use crate::config::SCHEMA_PATH;
        use crate::schema_check;
        const ADDRESS_OBJECT: &str = "/masterNode";
        let get_address_topic = format!("{}{}{}", "+/get/request/", APP_NAME, ADDRESS_OBJECT);
        let schema = schema_check::parse_schema(SCHEMA_PATH.join("get_address_schema.json")).ok();
        mqtt_msg_handler.add_topic_filter(
            get_address_topic,
            MqttTopicType::GetMasterAddress,
            schema,
        );

        let set_address_topic = format!("{}{}{}", "+/set/request/", APP_NAME, ADDRESS_OBJECT);
        let schema = schema_check::parse_schema(SCHEMA_PATH.join("set_address_schema.json")).ok();
        mqtt_msg_handler.add_topic_filter(
            set_address_topic,
            MqttTopicType::SetMasterAddress,
            schema,
        );
    }
}
