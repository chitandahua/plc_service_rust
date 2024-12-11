use chrono::{Datelike, Local, Timelike};
use num_enum::TryFromPrimitive;

use crate::protocol::app_data::{Afn, AppDataError};
use crate::protocol::user_data::dec_to_hex;
use crate::protocol::{Address, AppData};
use crate::Result;

// AFN 14H
#[derive(Debug, TryFromPrimitive)]
#[repr(u8)]
pub enum RouteDataRead {
    Clock = 2,
    CommDelay = 3,
}

pub struct ClockDataResponse;

impl From<ClockDataResponse> for AppData {
    fn from(_: ClockDataResponse) -> Self {
        // 获取当前时间
        let now = Local::now();
        let data = vec![
            dec_to_hex(now.second() as u8),       // 秒
            dec_to_hex(now.minute() as u8),       // 分
            dec_to_hex(now.hour() as u8),         // 时
            dec_to_hex(now.day() as u8),          // 日
            dec_to_hex(now.month() as u8),        // 月
            dec_to_hex((now.year() % 100) as u8), // 年份后两位
        ];
        AppData::new(Afn::RouteDataRead, RouteDataRead::Clock as u8, Some(data))
    }
}

const PREFIX_LEN: usize = 9;
pub struct CommDelayRequest {
    pub _node_addr: Address,
    pub delay: u16,
    pub _message_len: u8,
    pub _message: Vec<u8>,
}

impl TryFrom<AppData> for CommDelayRequest {
    type Error = crate::Error;
    fn try_from(app_data: AppData) -> Result<Self> {
        anyhow::ensure!(
            app_data.data_length() >= PREFIX_LEN,
            AppDataError::DataLength(app_data.data_length())
        );
        let message_len = app_data.data_units.as_ref().unwrap()[8] as usize;
        app_data.check(
            Afn::RouteDataRead,
            RouteDataRead::CommDelay as u8,
            PREFIX_LEN + message_len,
        )?;

        let data_units = app_data.data_units.unwrap();
        Ok(CommDelayRequest {
            _node_addr: Address::try_from(&data_units[0..6])?,
            delay: u16::from_le_bytes([data_units[6], data_units[7]]),
            _message_len: message_len as u8,
            _message: data_units[PREFIX_LEN..].to_vec(),
        })
    }
}

pub struct CommDelayResponse {
    message_len: u8,
    message: Vec<u8>,
}

impl CommDelayResponse {
    pub fn new(message: Vec<u8>) -> Self {
        CommDelayResponse {
            message_len: message.len() as u8,
            message,
        }
    }
}

impl From<CommDelayResponse> for AppData {
    fn from(response: CommDelayResponse) -> Self {
        let mut data = Vec::new();
        data.push(response.message_len);
        data.extend(response.message);
        AppData::new(
            Afn::RouteDataRead,
            RouteDataRead::CommDelay as u8,
            Some(data),
        )
    }
}
