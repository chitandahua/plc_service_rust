use crate::Result;
use anyhow::{bail, ensure};
use num_enum::{IntoPrimitive, TryFromPrimitive};
use std::fmt::Formatter;
use std::fmt::{self, Display};
use strum_macros::{EnumString, ToString};
use thiserror::Error;

mod answer;
pub use answer::{AnswerFn, ConfirmResponse, DenyErrorCode, DenyResponse};

mod ctrl_cmd;
pub use ctrl_cmd::{AddressSetRequest, CtrlCmd};

mod init;
pub use init::{InitOperation, InitRequest};

mod query_data;
pub use query_data::{ModuleInfoRequest, ModuleInfoResponse};

mod route_data_forward;
pub use route_data_forward::{DataForward, MonitorNodeRequest, MonitorNodeResponse};

mod route_get;
pub use route_get::{
    QueryNodeInfoRequest, QueryNodeInfoResponse, QueryNodeNumberRequest, QueryNodeNumberResponse,
    RouteQuery,
};

mod route_set;
pub use route_set::{AddNodeRequest, DelNodeRequest, RouteSet};

pub type Address = [u8; 6];

//impl Display for Address {
//    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
//        for &value in self.iter() {
//            write!(f, "{:02}", hex_to_dec(value))?;
//        }
//        Ok(())
//    }
//}

const AFN_SIZE: usize = 1;
const DATA_FLAG_SIZE: usize = 2;

#[derive(Debug, PartialEq, Clone, IntoPrimitive, TryFromPrimitive, strum_macros::Display)]
#[repr(u8)]
pub enum Afn {
    Answer = 0x00,
    Init = 0x01,
    DataForward = 0x02,
    QueryData = 0x03,
    PortCheck = 0x04,
    CtrlCmd = 0x05,
    Report = 0x06,
    RouteGet = 0x10,
    RouteSet = 0x11,
    RouteCtrl = 0x12,
    RouteDataForward = 0x13,
    RouteDataRead = 0x14,
    FileTransfer = 0x15,
    Debug = 0xf0,
    CocurrentReadMeter = 0xf1,
}

#[derive(Debug, Clone)]
pub struct DataFlag {
    type_: u8,
    mark: u8,
}

impl DataFlag {
    fn new(type_: u8, mark: u8) -> Self {
        Self { type_, mark }
    }

    fn as_fn_num(&self) -> u8 {
        self.type_ * 8 + self.mark
    }
}

impl From<u8> for DataFlag {
    fn from(fn_num: u8) -> Self {
        Self {
            type_: fn_num / 8,
            mark: fn_num % 8,
        }
    }
}

impl From<&[u8]> for DataFlag {
    fn from(data_flag: &[u8]) -> Self {
        let bytes: u16 = u16::from_le_bytes(data_flag.try_into().unwrap());
        Self {
            type_: (bytes >> 8) as u8,
            mark: bytes as u8,
        }
    }
}

#[derive(Debug, Clone)]
pub struct AppData {
    afn: Afn,
    data_flag: DataFlag,
    pub(crate) data_units: Option<Vec<u8>>,
}

impl AppData {
    pub fn new(afn: Afn, fn_num: u8, data_units: Option<Vec<u8>>) -> Self {
        Self {
            afn,
            data_flag: fn_num.into(),
            data_units,
        }
    }

    pub fn length(&self) -> usize {
        let mut length = AFN_SIZE + DATA_FLAG_SIZE;
        if let Some(data_units) = &self.data_units {
            length += data_units.len();
        }
        length
    }

    pub fn get_comm_mark(&self) -> u8 {
        match self.afn {
            Afn::Answer
            | Afn::Init
            | Afn::QueryData
            | Afn::CtrlCmd
            | Afn::RouteGet
            | Afn::RouteSet => 0,
            Afn::DataForward | Afn::RouteDataForward => 1,
            _ => 0,
        }
    }

    pub fn afn(&self) -> Afn {
        self.afn.clone()
    }

    pub fn fn_num(&self) -> u8 {
        self.data_flag.as_fn_num()
    }

    pub fn data_length(&self) -> usize {
        self.data_units
            .as_ref()
            .map_or(0, |data_units| data_units.len())
    }

    pub fn check(&self, afn: Afn, fn_num: u8, length: usize) -> Result<()> {
        ensure!(self.afn() == afn, AppDataError::Afn(self.afn()));
        ensure!(self.fn_num() == fn_num, AppDataError::FnNum(self.fn_num()));
        ensure!(
            self.data_length() == length,
            AppDataError::DataLength(self.data_length())
        );

        Ok(())
    }
}

impl TryFrom<&[u8]> for AppData {
    type Error = crate::Error;
    fn try_from(data: &[u8]) -> Result<Self> {
        let length = AFN_SIZE + DATA_FLAG_SIZE;
        ensure!(data.len() >= length, "app data length error");

        Ok(Self::new(
            data[0].try_into()?,
            DataFlag::from(&data[AFN_SIZE..length]).as_fn_num(),
            if data.len() > length {
                Some(data[length..].to_vec())
            } else {
                None
            },
        ))
    }
}

impl From<AppData> for Vec<u8> {
    fn from(app_data: AppData) -> Self {
        let mut data = vec![
            app_data.afn as u8,
            app_data.data_flag.mark,
            app_data.data_flag.type_,
        ];
        if let Some(data_units) = app_data.data_units {
            data.extend(data_units);
        }
        data
    }
}

impl Display for AppData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(
            f,
            "afn: {}, fn: {}",
            self.afn() as u8,
            self.data_flag.as_fn_num()
        )?;
        writeln!(
            f,
            "type: {}, mark: {}",
            self.data_flag.type_, self.data_flag.mark
        )?;
        writeln!(
            f,
            "data_units: {}",
            hex::encode(self.data_units.as_ref().unwrap_or(&vec![]))
        )
    }
}

impl IntoIterator for AppData {
    type Item = u8;
    type IntoIter = std::vec::IntoIter<Self::Item>;
    fn into_iter(self) -> Self::IntoIter {
        Into::<Vec<u8>>::into(self).into_iter()
    }
}

#[derive(Error, Debug)]
pub(crate) enum AppDataError {
    #[error("invalid afn {0}")]
    Afn(Afn),
    #[error("invalid fn {0}")]
    FnNum(u8),
    #[error("invalid data unit length {0}")]
    DataLength(usize),
}
