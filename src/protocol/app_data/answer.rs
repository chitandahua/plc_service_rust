use num_enum::{IntoPrimitive, TryFromPrimitive};
use strum_macros::{EnumString, ToString};

use std::fmt::Formatter;
use std::fmt::{self, Display};

use crate::protocol::app_data::Afn;
use crate::protocol::AppData;
use crate::Result;

// AFN 00H
#[derive(Debug)]
pub enum AnswerFn {
    Confirm = 1,
    Deny = 2,
}

#[derive(Debug, PartialEq)]
pub struct ConfirmResponse {
    channel_status: u32,
    wait_time: u16,
}

impl TryFrom<AppData> for ConfirmResponse {
    type Error = crate::Error;
    fn try_from(app_data: AppData) -> Result<Self> {
        app_data.check(Afn::Answer, AnswerFn::Confirm as u8, 6)?;

        let data_units = app_data.data_units.unwrap();
        Ok(ConfirmResponse {
            channel_status: u32::from_be_bytes(data_units[0..4].try_into().unwrap()),
            wait_time: u16::from_le_bytes(data_units[4..6].try_into().unwrap()),
        })
    }
}

impl From<ConfirmResponse> for AppData {
    fn from(response: ConfirmResponse) -> Self {
        let mut data_units = vec![];
        data_units.extend(response.channel_status.to_be_bytes());
        data_units.extend(response.wait_time.to_le_bytes());
        AppData::new(Afn::Answer, AnswerFn::Confirm as u8, Some(data_units))
    }
}

impl Display for ConfirmResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(
            f,
            "channel_status: 0x{:08x}, wait_time: {}",
            self.channel_status, self.wait_time
        )
    }
}

// Deny
#[derive(
    Debug, EnumString, TryFromPrimitive, IntoPrimitive, Clone, strum_macros::Display, PartialEq,
)]
#[repr(u8)]
pub enum DenyErrorCode {
    #[strum(serialize = "Time Out")]
    TimeOut = 0,
    #[strum(serialize = "Invalid ID")]
    InvalidDataUnit = 1,
    #[strum(serialize = "Error Length")]
    LengthError = 2,
    #[strum(serialize = "Error Checksum")]
    ChecksumError = 3,
    #[strum(serialize = "Error Data Info")]
    InfoClassNotExist = 4,
    #[strum(serialize = "Error Format")]
    FormatError = 5,
    #[strum(serialize = "Meter Has Existed")]
    MeterDup = 6,
    #[strum(serialize = "Meter Not Exist")]
    MeterNotExist = 7,
    #[strum(serialize = "Meter No Response")]
    AppNoResponse = 8,
    #[strum(serialize = "CCO Busy")]
    MasterBusy = 9,
    #[strum(serialize = "CCO Not Support")]
    MasterNotSupport = 10,
    #[strum(serialize = "Slave No Response")]
    SlaveNoResponse = 11,
    #[strum(serialize = "Slave Out Net")]
    SlaveOutNet = 12,
    #[strum(serialize = "Exceed Cocurrent Num")]
    ExceedCocurrentNum = 109,
    #[strum(serialize = "Exceed Msg Num")]
    ExceedMsgNum = 110,
    #[strum(serialize = "Meter Operating")]
    MeterOperating = 111,
    #[strum(serialize = "Other Error")]
    Other = 255,
}

#[derive(Debug, PartialEq)]
pub struct DenyResponse {
    error_code: DenyErrorCode,
}

impl TryFrom<AppData> for DenyResponse {
    type Error = crate::Error;
    fn try_from(app_data: AppData) -> Result<Self> {
        app_data.check(Afn::Answer, AnswerFn::Deny as u8, 1)?;
        Ok(DenyResponse {
            error_code: DenyErrorCode::try_from(app_data.data_units.unwrap()[0])?,
        })
    }
}

impl From<DenyResponse> for AppData {
    fn from(response: DenyResponse) -> Self {
        AppData::new(
            Afn::Answer,
            AnswerFn::Deny as u8,
            Some(vec![response.error_code as u8]),
        )
    }
}

impl Display for DenyResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "error_code: {}", self.error_code.clone() as u8)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::app_data::*;
    use crate::protocol::Frame;

    #[test]
    fn test_confirm_response_into() {
        let frame_str = "681500030000000000000001000000000006000a16";
        let frame = tests_common::create_frame_from_hex(frame_str);

        let confirm = ConfirmResponse {
            channel_status: 0x00000000,
            wait_time: 0x0006,
        };
        assert_eq!(frame.into_app_data(), confirm.into());
    }

    #[test]
    fn test_confirm_response_from() {
        let frame_str = "68150003000000000000000100403020100a00ae16";
        let frame = tests_common::create_frame_from_hex(frame_str);

        let confirm = ConfirmResponse {
            channel_status: 0x40302010,
            wait_time: 0x000a,
        };
        assert_eq!(
            TryInto::<ConfirmResponse>::try_into(frame.into_app_data()).unwrap(),
            confirm
        );
    }

    #[test]
    fn test_deny_response_into() {
        let frame_str = "68100003000000000001000200080e16";
        let frame = tests_common::create_frame_from_hex(frame_str);

        let deny = DenyResponse {
            error_code: DenyErrorCode::AppNoResponse,
        };
        assert_eq!(frame.into_app_data(), deny.into());
    }

    #[test]
    fn test_deny_response_from() {
        let frame_str = "681000030000000000010002006f7516";
        let frame = tests_common::create_frame_from_hex(frame_str);

        let deny = DenyResponse {
            error_code: DenyErrorCode::MeterOperating,
        };
        assert_eq!(
            TryInto::<DenyResponse>::try_into(frame.into_app_data()).unwrap(),
            deny
        );
    }
}
