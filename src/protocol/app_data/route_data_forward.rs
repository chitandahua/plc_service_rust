use std::fmt::Formatter;
use std::fmt::{self, Display};
use anyhow::ensure;

use crate::protocol::AppData;
use crate::protocol::app_data::{Afn, Address};

pub enum DataForward {
    MonitorNode = 1,
}

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
            Vec::with_capacity(4 + req.node_addrs.len() * Address::LEN + req.message.len());
        data.push(req.protocol_type);
        data.push(req.comm_delay_flag);
        data.push(req.node_addrs.len() as u8);
        // 高低位互换
        for addr in req.node_addrs {
            addr.reverse();
            data.extend(addr);
        }
        data.push(req.message.len() as u8);
        data.extend(req.message);
        AppData::new(Afn::RouteDataForward, DataForward::MonitorNode as u8, Some(data))
    }
}

pub struct MonitorNodeResponse {
    up_time: u16,
    protocol_type: u8,
    message_len: u8,
    message: Vec<u8>,
}

impl TryFrom<AppData> for MonitorNodeResponse {
    type Error = crate::Error;

    fn try_from(app_data: AppData) -> Result<Self, Self::Error> {
        const PREFIX_LEN: usize = 4;
        ensure!(
            app_data.data_length() >= PREFIX_LEN,
            AppDataError::DataLength(app_data.data_length())
        );
        let message_len = app_data.data_units.unwrap()[3] as usize;
        app_data.check(
            Afn::RouteDataForward,
            DataForward::MonitorNode as u8,
            PREFIX_LEN + message_len,
        )?;

        let data_units = app_data.data_units.unwrap();
        Ok(MonitorNodeResponse {
            up_time: u16::from_le_bytes(data_units[0..2].try_into().unwrap()),
            protocol_type: data_units[2],
            message_len,
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
