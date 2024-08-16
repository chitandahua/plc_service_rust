use num_enum::{FromPrimitive, IntoPrimitive};
use strum_macros::{EnumString, ToString};

use std::fmt::Formatter;
use std::fmt::{self, Display};

use crate::protocol::AppData;
use crate::protocol::app_data::Afn;

#[derive(Debug)]
pub enum AnswerFn {
    Confirm = 1,
    Deny = 2,
}

pub struct ConfirmResponse {
    channel_status: u32,
    wait_time: u16,
}

impl TryFrom<AppData> for ConfirmResponse {
    type Error = crate::Error;
    fn try_from(app_data: AppData) -> Result<Self, Self::Error> {
        app_data.check(Afn::Answer, AnswerFn::Confirm as u8, 6)?;

        let data_units = app_data.data_units.unwrap();
        Ok(ConfirmResponse {
            channel_status: u32::from_le_bytes(
                data_units[0..4].try_into().unwrap(),
            ),
            wait_time: u16::from_le_bytes(data_units[4..6].try_into().unwrap()),
        })
    }
}

impl From<ConfirmResponse> for AppData {
    fn from(response: ConfirmResponse) -> Self {
        let data_units = [
            response.channel_status.to_le_bytes(),
            response.wait_time.to_le_bytes(),
        ]
        .concat();
        AppData::new(
            Afn::Answer,
            AnswerFn::Confirm as u8,
            Some(data_units),
        )
    }
}

impl Display for ConfirmResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f,
            "channel_status: 0x{:08x}, wait_time: {}",
            self.channel_status,
            self.wait_time
        )
    }
}

// Deny
#[derive(Debug, EnumString, ToString, FromPrimitive, IntoPrimitive, Clone)]
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

pub struct DenyResponse {
    error_code: DenyErrorCode,
}

impl TryFrom<AppData> for DenyResponse {
    type Error = crate::Error;
    fn try_from(app_data: AppData) -> Result<Self, Self::Error> {
        app_data.check(Afn::Answer, AnswerFn::Deny as u8, 1)?;
        Ok(DenyResponse {
            error_code: app_data.data_units.unwrap()[0].try_into()?,
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
