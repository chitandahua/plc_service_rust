use std::sync::{mpsc, Arc};
use tracing::debug;

use crate::mqtt_message::Status;
use crate::protocol::app_data::{
    ActiveReport, Afn, CtrlCmd, DataForward, InitOperation, MeterControl, MeterReading, QueryData,
    RouteDataRead, RouteQuery, RouteSet,
};
use crate::request_info::{MqttReqInfo, ReqInfo};
use crate::service::{
    DebugMethod, EventReport, HplcInfo, ModuleService, PlcInit, RouteDataRequest,
};
use crate::{ModuleInfo, MqttMessage, MqttPayload};
use crate::{MqttResponseError, Result, UartHandler, UartMessage};

pub struct UartMsgHandler {
    mqtt_msg_sender: mpsc::Sender<MqttMessage>,
    uart_msg_sender: mpsc::Sender<UartMessage>,
    concurrent_msg_sender: mpsc::Sender<UartMessage>,
    services: ModuleService,
    plc_init: Arc<PlcInit>,
}

impl UartMsgHandler {
    pub fn new(
        mqtt_msg_sender: mpsc::Sender<MqttMessage>,
        uart_msg_sender: mpsc::Sender<UartMessage>,
        concurrent_msg_sender: mpsc::Sender<UartMessage>,
        services: ModuleService,
        plc_init: Arc<PlcInit>,
    ) -> Self {
        Self {
            mqtt_msg_sender,
            uart_msg_sender,
            concurrent_msg_sender,
            services,
            plc_init,
        }
    }

    fn uart_route_data_read_handler(&mut self, message: UartMessage) -> Result<()> {
        let (afn, fn_num) = message.req_info.frame_key().to_tuple();
        let fn_num = RouteDataRead::try_from(fn_num)
            .map_err(|_| UartHandlerError::UnsupportedAfnFn(afn, fn_num))?;
        match fn_num {
            RouteDataRead::Clock => {
                RouteDataRequest::uart_clock_data_response(message, &self.uart_msg_sender)
            }
            RouteDataRead::CommDelay => RouteDataRequest::uart_route_delay_response(
                message,
                self.services.monitor_node.clone(),
                &self.uart_msg_sender,
            ),
        }
    }

    fn uart_active_report_handler(&mut self, message: UartMessage) -> Result<()> {
        let (afn, fn_num) = message.req_info.frame_key().to_tuple();
        let fn_num = ActiveReport::try_from(fn_num)
            .map_err(|_| UartHandlerError::UnsupportedAfnFn(afn, fn_num))?;
        match fn_num {
            ActiveReport::SlaveNodeEvent => EventReport::uart_slave_node_event_report(
                message,
                &self.uart_msg_sender,
                &self.mqtt_msg_sender,
            ),
        }
    }

    fn uart_slave_report_handler(&mut self, message: UartMessage) -> Result<()> {
        let (afn, fn_num) = message.req_info.frame_key().to_tuple();
        match afn {
            Afn::QueryData => {
                let query_fn_num = QueryData::try_from(fn_num)
                    .map_err(|_| UartHandlerError::UnsupportedAfnFn(afn, fn_num))?;
                match query_fn_num {
                    QueryData::GetModuleInfo => {
                        match ModuleInfo::slave_module_info_report(message, &self.uart_msg_sender) {
                            Ok(address) => self.plc_init.update_address(address),
                            Err(e) => self.plc_init.notify(Err(e)),
                        }
                    }
                    _ => anyhow::bail!(UartHandlerError::UnsupportedAfnFn(afn, fn_num)),
                }
            }
            Afn::RouteDataRead => self.uart_route_data_read_handler(message)?,
            Afn::Report => self.uart_active_report_handler(message)?,
            _ => anyhow::bail!(UartHandlerError::UnsupportedAfn(afn)),
        }

        Ok(())
    }

    fn uart_init_handler(&mut self, message: UartMessage) -> Result<()> {
        let init_err_msg = "uart init invalid message";
        let (afn, fn_num) = message.req_info.frame_key().to_tuple();
        if afn == Afn::QueryData {
            let fn_num = QueryData::try_from(fn_num).expect(init_err_msg);
            match fn_num {
                QueryData::GetModuleInfo => match ModuleInfo::init_module_info_response(message) {
                    Ok(address) => self.plc_init.update_address(address),
                    Err(e) => self.plc_init.notify(Err(e)),
                },
                _ => unreachable!(),
            }
        } else {
            let result = match afn {
                Afn::CtrlCmd => {
                    let fn_num = CtrlCmd::try_from(fn_num).expect(init_err_msg);
                    match fn_num {
                        CtrlCmd::SetAddress => self
                            .services
                            .master_address
                            .init_set_address_response(message),
                    }
                }
                Afn::RouteSet => {
                    let fn_num = RouteSet::try_from(fn_num).expect(init_err_msg);
                    match fn_num {
                        RouteSet::AddNode => self.services.node_manage.uart_add_acq_files(message),
                        _ => unreachable!(),
                    }
                }
                Afn::Init => {
                    let fn_num = InitOperation::try_from(fn_num).expect(init_err_msg);
                    match fn_num {
                        InitOperation::Params => {
                            self.services.node_manage.uart_clear_acq_files(message)
                        }
                        _ => unreachable!(),
                    }
                }
                // 内部操作
                Afn::RouteCtrl => {
                    let _ = MeterControl::try_from(fn_num).expect("invalid route ctrl fn");
                    self.services.route_ctrl.uart_operate_metering(message)
                }
                _ => unreachable!(),
            };
            self.plc_init.notify(result);
        }

        Ok(())
    }

    fn uart_query_data_handler(&mut self, message: UartMessage) -> Result<()> {
        let (afn, fn_num) = message.req_info.frame_key().to_tuple();
        let fn_num = QueryData::try_from(fn_num)
            .map_err(|_| UartHandlerError::UnsupportedAfnFn(afn, fn_num))?;
        match fn_num {
            QueryData::GetModuleInfo => {
                ModuleInfo::module_info_response(message, &self.mqtt_msg_sender)
            }
            QueryData::GetMasterIdInfo => {
                ModuleInfo::master_id_info_response(message, &self.mqtt_msg_sender)
            }
        }
    }

    fn uart_ctrl_cmd_handler(&mut self, message: UartMessage) -> Result<()> {
        let (afn, fn_num) = message.req_info.frame_key().to_tuple();
        let fn_num = CtrlCmd::try_from(fn_num)
            .map_err(|_| UartHandlerError::UnsupportedAfnFn(afn, fn_num))?;
        match fn_num {
            CtrlCmd::SetAddress => self
                .services
                .master_address
                .uart_set_address(message, &self.mqtt_msg_sender)?,
        }
        Ok(())
    }

    fn uart_route_set_handler(&mut self, message: UartMessage) -> Result<()> {
        let (afn, fn_num) = message.req_info.frame_key().to_tuple();
        let fn_num = RouteSet::try_from(fn_num)
            .map_err(|_| UartHandlerError::UnsupportedAfnFn(afn, fn_num))?;
        match fn_num {
            RouteSet::AddNode => self.services.node_manage.uart_add_acq_files(message)?,
            RouteSet::DelNode => self.services.node_manage.uart_del_acq_files(message)?,
        }
        Ok(())
    }

    fn uart_route_query_handler(&mut self, message: UartMessage) -> Result<()> {
        let (afn, fn_num) = message.req_info.frame_key().to_tuple();
        let fn_num = RouteQuery::try_from(fn_num)
            .map_err(|_| UartHandlerError::UnsupportedAfnFn(afn, fn_num))?;
        match fn_num {
            RouteQuery::NodeNumber => self
                .services
                .node_manage
                .uart_get_acq_files_number(message, &self.mqtt_msg_sender),
            RouteQuery::NodeInfo => self
                .services
                .node_manage
                .uart_get_acq_files(message, &self.mqtt_msg_sender),
            RouteQuery::NodeLineInfo => {
                HplcInfo::uart_node_line_info_response(message, &self.mqtt_msg_sender)
            }
            RouteQuery::ChipInfo => HplcInfo::chip_info_response(message, &self.mqtt_msg_sender),
            RouteQuery::IdInfo => HplcInfo::uart_id_info_response(message, &self.mqtt_msg_sender),
            RouteQuery::SlaveModuleId => {
                HplcInfo::uart_slave_module_id_info_response(message, &self.mqtt_msg_sender)
            }
        }
    }

    fn uart_route_ctrl_handler(&mut self, message: UartMessage) -> Result<()> {
        let (afn, fn_num) = message.req_info.frame_key().to_tuple();
        let fn_num = MeterControl::try_from(fn_num)
            .map_err(|_| UartHandlerError::UnsupportedAfnFn(afn, fn_num))?;
        match fn_num {
            MeterControl::Pause => self.services.route_ctrl.uart_operate_metering(message),
            MeterControl::Resume => self.services.route_ctrl.uart_operate_metering(message),
            MeterControl::Restart => self.services.route_ctrl.uart_operate_metering(message),
        }
    }

    fn uart_route_data_forward_handler(&mut self, message: UartMessage) -> Result<()> {
        let (afn, fn_num) = message.req_info.frame_key().to_tuple();
        let fn_num = DataForward::try_from(fn_num)
            .map_err(|_| UartHandlerError::UnsupportedAfnFn(afn, fn_num))?;
        match fn_num {
            DataForward::MonitorNode => self
                .services
                .monitor_node
                .uart_get_monitor_node_data(message),
        }
    }

    fn uart_read_meter_handler(&mut self, message: UartMessage) -> Result<()> {
        let (afn, fn_num) = message.req_info.frame_key().to_tuple();
        let fn_num = MeterReading::try_from(fn_num)
            .map_err(|_| UartHandlerError::UnsupportedAfnFn(afn, fn_num))?;
        match fn_num {
            MeterReading::ActiveReadMeter => {
                let master_address = self.services.master_address.get_master_address();
                self.services.concurrent_meter.uart_meter_reading(
                    message,
                    master_address,
                    &self.mqtt_msg_sender,
                    &self.concurrent_msg_sender,
                )?;
            }
        }
        Ok(())
    }

    fn uart_test_handler(&mut self, message: UartMessage) -> Result<()> {
        let (_afn, fn_num) = message.req_info.frame_key().to_tuple();
        match fn_num {
            0 => DebugMethod::uart_debug_frame_response(message, &self.mqtt_msg_sender),
            _ => unreachable!(),
        }
    }

    fn uart_mqtt_handler(&mut self, message: UartMessage) -> Result<()> {
        let afn = message.req_info.frame_key().afn();
        match afn {
            Afn::CtrlCmd => self.uart_ctrl_cmd_handler(message),
            Afn::QueryData => self.uart_query_data_handler(message),
            Afn::RouteSet => self.uart_route_set_handler(message),
            Afn::RouteGet => self.uart_route_query_handler(message),
            Afn::RouteCtrl => self.uart_route_ctrl_handler(message),
            Afn::RouteDataForward => self.uart_route_data_forward_handler(message),
            Afn::CocurrentReadMeter => self.uart_read_meter_handler(message),
            Afn::Test => self.uart_test_handler(message),
            _ => anyhow::bail!(UartHandlerError::UnsupportedAfn(afn)),
        }
    }
}

impl UartHandler for UartMsgHandler {
    fn uart_msg_handler(&mut self, message: UartMessage) -> Result<()> {
        debug!(
            "uart msg response handler: AFN: {:#02x}, Fn: {}",
            message.req_info.frame_key().afn() as u8,
            message.req_info.frame_key().fn_num()
        );

        match message.frame.is_slave_report() {
            false => match message.req_info.is_init() {
                false => self.uart_mqtt_handler(message),
                true => self.uart_init_handler(message),
            },
            true => self.uart_slave_report_handler(message),
        }?;

        Ok(())
    }
}

#[derive(Clone)]
pub struct UartTimeoutHandler {
    mqtt_msg_sender: mpsc::Sender<MqttMessage>,
    concurrent_msg_sender: mpsc::Sender<UartMessage>,
    services: ModuleService,
    plc_init: Arc<PlcInit>,
}

impl UartTimeoutHandler {
    pub fn new(
        mqtt_msg_sender: mpsc::Sender<MqttMessage>,
        concurrent_msg_sender: mpsc::Sender<UartMessage>,
        services: ModuleService,
        plc_init: Arc<PlcInit>,
    ) -> Self {
        Self {
            mqtt_msg_sender,
            concurrent_msg_sender,
            services,
            plc_init,
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
            (Afn::RouteSet, _) => {
                self.services.node_manage.uart_operate_acq_files_timeout();
            }
            (Afn::RouteDataForward, 1) => {
                todo!()
            }
            _ => match req_info.into_mqtt_req_info() {
                Some(mqtt_req_info) => self.mqtt_timeout_cb(mqtt_req_info),
                None => self.plc_init.notify_timeout(),
            },
        }

        Ok(())
    }
}

#[derive(thiserror::Error, Debug)]
pub enum UartHandlerError {
    #[error("unsupported afn: {:#02x}", *.0 as u8)]
    UnsupportedAfn(Afn),
    #[error("unsupported afn: {:#02x}, fn: {1}", *.0 as u8)]
    UnsupportedAfnFn(Afn, u8),
}
