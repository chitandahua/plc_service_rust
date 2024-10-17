use std::sync::mpsc;

use crate::protocol::app_data::{Afn, ClockDataResponse, CommDelayRequest, CommDelayResponse};
use crate::protocol::Frame;
use crate::request_info::{ReqInfo, UartMessage};
use crate::service::MonitorNode;
use crate::Result;

pub struct RouteDataRequest;

impl RouteDataRequest {
    pub fn uart_clock_data_response(
        message: UartMessage,
        uart_msg_sender: &mpsc::Sender<UartMessage>,
    ) -> Result<()> {
        let frame = Frame::new_response(message.frame.get_seq(), None, ClockDataResponse);
        let response = UartMessage::new(ReqInfo::new(&message.frame, None), frame);
        uart_msg_sender.send(response).unwrap();
        Ok(())
    }

    pub fn uart_route_delay_response(
        mut message: UartMessage,
        monitor_node: MonitorNode,
        uart_msg_sender: &mpsc::Sender<UartMessage>,
    ) -> Result<()> {
        //let req_info = ReqInfo::new(&message.frame, None);
        let frame = Frame::new_response(
            message.frame.get_seq(),
            None,
            CommDelayResponse::new(vec![]),
        );

        let init_req_info = message.extra_req_info.unwrap();
        match init_req_info.frame_key().to_tuple() {
            (Afn::RouteDataForward, 1) => {
                let request = CommDelayRequest::try_from(message.frame.into_app_data())?;
                monitor_node.uart_notify_monitor_node_dalay(request.delay);
            }
            _ => unreachable!(),
        }

        let response = UartMessage::new(init_req_info, frame);
        uart_msg_sender.send(response).unwrap();

        Ok(())
    }
}
