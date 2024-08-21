use anyhow::ensure;
use std::fmt::Formatter;
use std::fmt::{self, Display};

use crate::protocol::app_data::{Address, Afn, AppDataError, ADDR_LEN};
use crate::protocol::AppData;
use crate::Result;

// AFN 13H
pub enum DataForward {
    MonitorNode = 1,
}

const PREFIX_LEN: usize = 4;
#[derive(Debug)]
pub struct MonitorNodeRequest {
    protocol_type: u8,
    comm_delay_flag: u8,
    node_addrs: Vec<Address>,
    message: Vec<u8>,
}

impl From<MonitorNodeRequest> for AppData {
    fn from(req: MonitorNodeRequest) -> Self {
        let mut data =
            Vec::with_capacity(PREFIX_LEN + req.node_addrs.len() * ADDR_LEN + req.message.len());
        data.push(req.protocol_type);
        data.push(req.comm_delay_flag);
        data.push(req.node_addrs.len() as u8);
        // 高低位互换
        for addr in req.node_addrs {
            data.extend(addr);
        }
        data.push(req.message.len() as u8);
        data.extend(req.message);
        AppData::new(
            Afn::RouteDataForward,
            DataForward::MonitorNode as u8,
            Some(data),
        )
    }
}

#[derive(Debug, PartialEq)]
pub struct MonitorNodeResponse {
    up_time: u16,
    protocol_type: u8,
    message_len: u8,
    message: Vec<u8>,
}

impl TryFrom<AppData> for MonitorNodeResponse {
    type Error = crate::Error;

    fn try_from(app_data: AppData) -> Result<Self> {
        ensure!(
            app_data.data_length() >= PREFIX_LEN,
            AppDataError::DataLength(app_data.data_length())
        );
        let message_len = app_data.data_units.as_ref().unwrap()[3] as usize;
        app_data.check(
            Afn::RouteDataForward,
            DataForward::MonitorNode as u8,
            PREFIX_LEN + message_len,
        )?;

        let data_units = app_data.data_units.unwrap();
        Ok(MonitorNodeResponse {
            up_time: u16::from_le_bytes(data_units[0..2].try_into().unwrap()),
            protocol_type: data_units[2],
            message_len: message_len as u8,
            message: data_units[PREFIX_LEN..].to_vec(),
        })
    }
}

impl Display for MonitorNodeResponse {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(
            f,
            "up_time: {}, protocol_type: {}",
            self.up_time, self.protocol_type
        )?;
        writeln!(
            f,
            "message_len: {}, message: {}",
            self.message_len,
            String::from_utf8_lossy(&self.message)
        )
    }
}

#[cfg(test)]
mod tests {
    use tests_common::create_frame_from_hex;

    use super::*;
    use crate::protocol::app_data::*;
    use crate::protocol::Frame;

    #[test]
    fn test_monitor_node_request() {
        let frame_str = "68390043040000000000ab8967563412ab8967564321130100020002ab8967563413ab89675634140e6812345678901268010243c3ac16bb16";
        let frame = tests_common::create_frame_from_hex(frame_str);

        let monitor_node_request = MonitorNodeRequest {
            protocol_type: 0x02,
            comm_delay_flag: 0x00,
            node_addrs: vec![
                Address::new([0x13, 0x34, 0x56, 0x67, 0x89, 0xab]),
                Address::new([0x14, 0x34, 0x56, 0x67, 0x89, 0xab]),
            ],
            message: tests_common::hex_to_bytes("6812345678901268010243c3ac16"),
        };

        assert_eq!(frame.into_app_data(), monitor_node_request.into());
    }

    #[test]
    fn test_monitor_node_response() {
        let frame_str = "682100830000000000041301000b00020e6812345678901268010243c3ac160716";
        let frame = tests_common::create_frame_from_hex(frame_str);
        let monitor_node_response = MonitorNodeResponse {
            up_time: 0x000b,
            protocol_type: 0x02,
            message_len: 0x0e,
            message: tests_common::hex_to_bytes("6812345678901268010243c3ac16"),
        };

        let response: MonitorNodeResponse = frame.into_app_data().try_into().unwrap();
        assert_eq!(response, monitor_node_response);
    }
}
