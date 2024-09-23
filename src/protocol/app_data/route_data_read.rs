use chrono::{Datelike, Local, Timelike};
use num_enum::TryFromPrimitive;

use crate::protocol::app_data::Afn;
use crate::protocol::AppData;

// AFN 14H
#[derive(Debug, TryFromPrimitive)]
#[repr(u8)]
pub enum RouteDataRead {
    Clock = 2,
}

pub struct ClockDataResponse;

fn dec_to_hex(value: u8) -> u8 {
    ((value / 10) % 10) * 16 + (value % 10)
}

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
