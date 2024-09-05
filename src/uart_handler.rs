use std::sync::mpsc;
use tracing::debug;

use crate::mqtt_message::Status;
use crate::protocol::app_data::Afn;
use crate::request_info::{MqttReqInfo, ReqInfo};
use crate::service::ModuleService;
use crate::{ModuleInfo, MqttMessage, MqttPayload};
use crate::{MqttResponseError, Result, UartHandler, UartMessage};

pub struct UartMsgHandler {
    mqtt_msg_sender: mpsc::Sender<MqttMessage>,
    uart_msg_sender: mpsc::Sender<UartMessage>,
    concurrent_msg_sender: mpsc::Sender<UartMessage>,
    services: ModuleService,
}

impl UartMsgHandler {
    pub fn new(
        mqtt_msg_sender: mpsc::Sender<MqttMessage>,
        uart_msg_sender: mpsc::Sender<UartMessage>,
        concurrent_msg_sender: mpsc::Sender<UartMessage>,
        services: ModuleService,
    ) -> Self {
        Self {
            mqtt_msg_sender,
            uart_msg_sender,
            concurrent_msg_sender,
            services,
        }
    }
}

impl UartHandler for UartMsgHandler {
    fn uart_msg_handler(&mut self, message: UartMessage) -> Result<()> {
        debug!(
            "uart msg handler: AFN: {:02x}, Fn: {}",
            message.req_info.frame_key().afn(),
            message.req_info.frame_key().fn_num()
        );

        match message.req_info.frame_key().to_tuple() {
            (Afn::QueryData, 10) => {
                ModuleInfo::module_info_response(
                    message,
                    &self.mqtt_msg_sender,
                    &self.uart_msg_sender,
                )?;
            }
            (Afn::CtrlCmd, 1) => {
                self.services
                    .master_address
                    .uart_set_address(message, &self.mqtt_msg_sender)?;
            }
            (Afn::RouteSet, 1) => {
                self.services.node_manage.uart_add_acq_files(message)?;
            }
            (Afn::RouteSet, 2) => {
                self.services.node_manage.uart_del_acq_files(message)?;
            }
            (Afn::RouteGet, 1) => self
                .services
                .node_manage
                .uart_get_acq_files_number(message, &self.mqtt_msg_sender)?,
            (Afn::RouteGet, 2) => self
                .services
                .node_manage
                .uart_get_acq_files(message, &self.mqtt_msg_sender)?,
            (Afn::CocurrentReadMeter, 1) => {
                let master_address = self.services.master_address.get_master_address();
                self.services.concurrent_meter.uart_meter_reading(
                    message,
                    master_address,
                    &self.mqtt_msg_sender,
                    &self.concurrent_msg_sender,
                )?;
            }
            _ => todo!(),
        }

        Ok(())
    }
}

#[derive(Clone)]
pub struct UartTimeoutHandler {
    mqtt_msg_sender: mpsc::Sender<MqttMessage>,
    concurrent_msg_sender: mpsc::Sender<UartMessage>,
    services: ModuleService,
}

impl UartTimeoutHandler {
    pub fn new(
        mqtt_msg_sender: mpsc::Sender<MqttMessage>,
        concurrent_msg_sender: mpsc::Sender<UartMessage>,
        services: ModuleService,
    ) -> Self {
        Self {
            mqtt_msg_sender,
            concurrent_msg_sender,
            services,
        }
    }

    fn mqtt_timeout_cb(&self, mqtt_req_info: MqttReqInfo) {
        let payload = MqttPayload::new(
            mqtt_req_info.token(),
            Status::Failure,
            MqttResponseError::Timeout,
            None,
        );
        self.mqtt_msg_sender
            .send(MqttMessage::new(mqtt_req_info.topic(), payload))
            .unwrap();
    }

    pub fn handle_timeout(&self, req_info: ReqInfo) -> Result<()> {
        match req_info.frame_key().to_tuple() {
            (Afn::CocurrentReadMeter, 1) => {
                let master_address = self.services.master_address.get_master_address();
                self.services.concurrent_meter.uart_meter_reading_timeout(
                    req_info.into_mqtt_req_info().unwrap(),
                    master_address,
                    &self.concurrent_msg_sender,
                    &self.mqtt_msg_sender,
                )?;
            }
            _ => match req_info.into_mqtt_req_info() {
                Some(mqtt_req_info) => self.mqtt_timeout_cb(mqtt_req_info),
                None => {}
            },
        }

        Ok(())
    }
}
