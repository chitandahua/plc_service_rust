use std::sync::mpsc;

use crate::protocol::app_data::ClockDataResponse;
use crate::protocol::Frame;
use crate::request_info::{ReqInfo, UartMessage};
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
}
