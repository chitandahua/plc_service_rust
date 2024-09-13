use anyhow::ensure;
use num_enum::TryFromPrimitive;
use std::fmt::Display;
use std::fmt::Formatter;

use crate::protocol::app_data::{Afn, AppDataError};
use crate::protocol::AppData;
use crate::Result;

// AFN F1H
#[derive(Debug, TryFromPrimitive)]
#[repr(u8)]
pub enum MeterReading {
    ActiveReadMeter = 1,
}

const REQ_PREFIX_LEN: usize = 4;
#[derive(Debug)]
pub struct ConcurrentReadMeterRequest {
    protocol_type: u8,
    reserve: u8,
    message: Vec<u8>,
}

impl ConcurrentReadMeterRequest {
    pub fn new(protocol_type: u8, message: Vec<u8>) -> Self {
        Self {
            protocol_type,
            reserve: 0,
            message,
        }
    }
}

impl From<ConcurrentReadMeterRequest> for AppData {
    fn from(req: ConcurrentReadMeterRequest) -> Self {
        let mut data = Vec::with_capacity(REQ_PREFIX_LEN + req.message.len());
        data.push(req.protocol_type);
        data.push(req.reserve);
        data.extend(u16::to_le_bytes(req.message.len() as u16));
        data.extend(req.message);
        AppData::new(
            Afn::CocurrentReadMeter,
            MeterReading::ActiveReadMeter as u8,
            Some(data),
        )
    }
}

const RES_PREFIX_LEN: usize = 3;
#[derive(Debug, PartialEq)]
pub struct ConcurrentReadMeterResponse {
    protocol_type: u8,
    message_len: u16,
    pub message: Vec<u8>,
}

impl TryFrom<AppData> for ConcurrentReadMeterResponse {
    type Error = crate::Error;

    fn try_from(app_data: AppData) -> Result<Self> {
        ensure!(
            app_data.data_length() >= RES_PREFIX_LEN,
            AppDataError::DataLength(app_data.data_length())
        );
        let message_len = u16::from_le_bytes(
            app_data.data_units.as_ref().unwrap()[1..3]
                .try_into()
                .unwrap(),
        );
        app_data.check(
            Afn::CocurrentReadMeter,
            MeterReading::ActiveReadMeter as u8,
            RES_PREFIX_LEN + message_len as usize,
        )?;

        let data_units = app_data.data_units.unwrap();
        Ok(ConcurrentReadMeterResponse {
            protocol_type: data_units[0],
            message_len,
            message: data_units[RES_PREFIX_LEN..].to_vec(),
        })
    }
}

impl Display for ConcurrentReadMeterResponse {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "protocol_type: {}", self.protocol_type)?;
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
    fn test_meter_read_request() {
        let frame_str = "682d0043040000000000ab8967563412ab8967564321f1010002000e006812345678901268010243c3ac162616";
        let frame = tests_common::create_frame_from_hex(frame_str);

        let meter_read_request = ConcurrentReadMeterRequest {
            protocol_type: 0x02,
            reserve: 0x00,
            message: tests_common::hex_to_bytes("6812345678901268010243c3ac16"),
        };

        assert_eq!(frame.into_app_data(), meter_read_request.into());
    }

    #[test]
    fn test_meter_read_response() {
        let frame_str = "683200830400101f0051222202005002129078563412f10100021400680250000222226891083333343337363333a116b516";
        let frame = tests_common::create_frame_from_hex(frame_str);
        let meter_read_response = ConcurrentReadMeterResponse {
            protocol_type: 0x02,
            message_len: 0x0014,
            message: tests_common::hex_to_bytes("680250000222226891083333343337363333a116"),
        };

        let response: ConcurrentReadMeterResponse = frame.into_app_data().try_into().unwrap();
        assert_eq!(response, meter_read_response);
    }
}
