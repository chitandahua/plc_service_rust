use std::sync::mpsc;
use tracing::debug;

use crate::protocol::app_data::Afn;
use crate::request_info::FrameKey;
use crate::service::ModuleService;
use crate::MqttMessage;
use crate::{Result, UartHandler, UartMessage};

use crate::ModuleInfo;

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
                ModuleInfo::module_info_response(message, &self.mqtt_msg_sender)?;
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
