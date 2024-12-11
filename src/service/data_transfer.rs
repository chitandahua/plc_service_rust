use serde::{Deserialize, Serialize};

use crate::mqtt_handler::MqttTopicType;
use crate::mqtt_message::PayloadBody;
use crate::protocol::app_data::{TransferFrameRequest, TransferFrameResponse};
use crate::protocol::{Address, AddressField, Frame};
use crate::{impl_into_mqtt_message, ReqInfo};

use crate::request_info::UartMessage;
use crate::service::parse_response::UartResponse;
use crate::{register_mqtt_request_topics, MqttMessage, MqttMsgHandler, MqttResponseError, Result};

use crate::service::{IntoMqttMessage, MqttReqInfo};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, Condvar, Mutex};
use std::time::Duration;

#[derive(Clone)]
pub struct DataTransfer {
    metering: Arc<AtomicBool>,
    result: Arc<Mutex<Option<Result<MqttDataTransferResponse>>>>,
    cond: Arc<Condvar>,
}

#[derive(Debug, Deserialize)]
struct MqttDataTransferRequest {
    #[serde(rename = "acqAddr")]
    acq_addr: String,
    #[serde(rename = "proType")]
    pro_type: u8,
    #[serde(rename = "frameTimeout")]
    frame_timeout: u32,
    #[serde(rename = "charTimeout")]
    _char_timeout: u32,
    data: String,
}

impl From<MqttDataTransferRequest> for TransferFrameRequest {
    fn from(req: MqttDataTransferRequest) -> Self {
        Self::new(req.pro_type, hex::decode(req.data).unwrap())
    }
}

#[derive(Debug, Serialize)]
struct MqttDataTransferResponse {
    data: String,
}

impl From<TransferFrameResponse> for MqttDataTransferResponse {
    fn from(response: TransferFrameResponse) -> Self {
        MqttDataTransferResponse {
            data: hex::encode(response.message),
        }
    }
}

impl_into_mqtt_message!(MqttDataTransferResponse, flat);

impl DataTransfer {
    pub fn new(metering: Arc<AtomicBool>) -> Self {
        Self {
            metering,
            result: Arc::new(Mutex::new(None)),
            cond: Arc::new(Condvar::new()),
        }
    }

    pub fn init(mqtt_msg_handler: &mut MqttMsgHandler) {
        register_mqtt_request_topics!(
            mqtt_msg_handler,
            (
                "get",
                "dataTrans",
                MqttTopicType::DataTransfer,
                "data_transfer_schema.json"
            ),
        )
    }

    fn with_metering<F, T>(&self, f: F) -> T
    where
        F: FnOnce() -> T,
    {
        self.metering.store(true, Ordering::Relaxed);
        let result = f();
        self.metering.store(false, Ordering::Relaxed);
        result
    }

    pub fn mqtt_data_transfer(
        &self,
        master_address: Address,
        message: MqttMessage,
        mqtt_msg_sender: &mpsc::Sender<MqttMessage>,
        uart_msg_sender: &mpsc::Sender<UartMessage>,
    ) -> Result<()> {
        let request: MqttDataTransferRequest = serde_json::from_str(message.payload()).unwrap();
        let mqtt_req_info = message.to_mqtt_req_info();
        let timeout = Duration::from_secs(request.frame_timeout as u64);

        let frame = Frame::new_request(
            Some(AddressField::new(
                master_address,
                None,
                Address::from(request.acq_addr.as_str()),
            )),
            TransferFrameRequest::from(request),
        );
        let req_info = ReqInfo::new(&frame, Some(mqtt_req_info));

        self.with_metering(move || {
            uart_msg_sender
                .send(UartMessage::new_with_timeout(req_info, frame, timeout))
                .unwrap();

            let mut result = self.result.lock().unwrap();
            result = self
                .cond
                .wait_while(result, |result| result.is_none())
                .unwrap();

            mqtt_msg_sender
                .send(
                    result
                        .take()
                        .unwrap()
                        .into_mqtt_message(message.to_mqtt_req_info()),
                )
                .unwrap()
        });

        Ok(())
    }

    fn notify(&self, result: Result<MqttDataTransferResponse>) {
        let mut lock = self.result.lock().unwrap();
        *lock = Some(result);
        self.cond.notify_one();
    }

    pub fn uart_data_transfer_response(&self, message: UartMessage) -> Result<()> {
        let response = UartResponse::<TransferFrameResponse>::try_from(message.frame)?;
        let response: Result<TransferFrameResponse> = response.into();
        self.notify(response.map(MqttDataTransferResponse::from));
        Ok(())
    }

    pub fn uart_data_transfer_timeout(&self) {
        self.notify(Err(anyhow::anyhow!(MqttResponseError::Timeout)));
    }
}
