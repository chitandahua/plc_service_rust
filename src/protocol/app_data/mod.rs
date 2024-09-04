use crate::Result;
use anyhow::ensure;
use num_enum::{IntoPrimitive, TryFromPrimitive};
use std::fmt::{Display, Formatter};

use thiserror::Error;

mod answer;
pub use answer::{AnswerFn, ConfirmResponse, DenyResponse};

mod ctrl_cmd;
pub use ctrl_cmd::AddressSetRequest;

mod init;
pub use init::{InitOperation, InitRequest};

mod meter_reading;
pub use meter_reading::{ConcurrentReadMeterRequest, ConcurrentReadMeterResponse};

mod query_data;
pub use query_data::{ModuleInfoRequest, ModuleInfoResponse};

//mod route_data_forward;
//pub use route_data_forward::{DataForward, MonitorNodeRequest, MonitorNodeResponse};

mod route_get;
pub use route_get::{
    NodeDetail, QueryNodeInfoRequest, QueryNodeInfoResponse, QueryNodeNumberRequest,
    QueryNodeNumberResponse,
};

mod route_set;
pub use route_set::{AddNodeRequest, DelNodeRequest, NodeInfo};

pub const ADDR_LEN: usize = 6;
#[derive(Debug, Clone, PartialEq)]
pub struct Address([u8; ADDR_LEN]);

impl Address {
    pub fn new(addr: [u8; ADDR_LEN]) -> Self {
        Self(addr)
    }
}

impl Display for Address {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", hex::encode(self.0))
    }
}

impl TryFrom<&[u8]> for Address {
    type Error = crate::Error;
    fn try_from(data: &[u8]) -> Result<Self> {
        ensure!(data.len() == ADDR_LEN, "address length error");
        //let mut address = [0u8; ADDR_LEN];
        //address.copy_from_slice(data);
        let address = data
            .iter()
            .rev()
            .cloned()
            .collect::<Vec<u8>>()
            .try_into()
            .unwrap();
        Ok(Self(address))
    }
}

impl From<&str> for Address {
    fn from(value: &str) -> Self {
        let bytes = hex::decode(value).expect("Invalid hex string");
        let mut address = [0u8; ADDR_LEN];
        address.copy_from_slice(&bytes);
        Self(address)
    }
}

impl From<Address> for Vec<u8> {
    fn from(address: Address) -> Self {
        address.0.into_iter().rev().collect()
    }
}

impl IntoIterator for Address {
    type Item = u8;
    type IntoIter = std::vec::IntoIter<Self::Item>;
    //type IntoIter = std::array::IntoIter<Self::Item, ADDR_LEN>;
    fn into_iter(self) -> Self::IntoIter {
        Into::<Vec<u8>>::into(self).into_iter()
        //self.0.into_iter().rev()
    }
}

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

#[derive(Debug, Clone, PartialEq)]
pub struct DataFlag {
    type_: u8,
    mark: u8,
}

impl DataFlag {
    fn _new(type_: u8, mark: u8) -> Self {
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

#[derive(Debug, Clone, PartialEq)]
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
            Afn::DataForward | Afn::RouteDataForward | Afn::CocurrentReadMeter => 1,
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::convert::TryFrom;

    #[test]
    fn test_app_data_creation() {
        let app_data = AppData::new(Afn::Init, 1, Some(vec![1, 2, 3]));
        assert_eq!(app_data.afn(), Afn::Init);
        assert_eq!(app_data.fn_num(), 1);
        assert_eq!(app_data.data_length(), 3);
    }

    #[test]
    fn test_app_data_length() {
        let app_data = AppData::new(Afn::CtrlCmd, 2, Some(vec![1, 2, 3, 4]));
        assert_eq!(app_data.length(), 7); // AFN_SIZE + DATA_FLAG_SIZE + data_units.len()
    }

    #[test]
    fn test_app_data_get_comm_mark() {
        let app_data1 = AppData::new(Afn::Init, 1, None);
        assert_eq!(app_data1.get_comm_mark(), 0);

        let app_data2 = AppData::new(Afn::DataForward, 1, None);
        assert_eq!(app_data2.get_comm_mark(), 1);
    }

    #[test]
    fn test_app_data_check() {
        let app_data = AppData::new(Afn::QueryData, 3, Some(vec![1, 2, 3]));
        assert!(app_data.check(Afn::QueryData, 3, 3).is_ok());
        assert!(app_data.check(Afn::Init, 3, 3).is_err());
        assert!(app_data.check(Afn::QueryData, 4, 3).is_err());
        assert!(app_data.check(Afn::QueryData, 3, 4).is_err());
    }

    #[test]
    fn test_app_data_try_from() {
        let data = vec![Afn::RouteGet as u8, 1, 0, 5, 6, 7];
        let app_data = AppData::try_from(data.as_slice()).unwrap();
        assert_eq!(app_data.afn(), Afn::RouteGet);
        assert_eq!(app_data.fn_num(), 1);
        assert_eq!(app_data.data_units, Some(vec![5, 6, 7]));
    }

    #[test]
    fn test_app_data_try_from_error() {
        let data = vec![0xFF]; // Invalid AFN
        assert!(AppData::try_from(data.as_slice()).is_err());

        let data = vec![Afn::Init as u8]; // Too short
        assert!(AppData::try_from(data.as_slice()).is_err());
    }

    #[test]
    fn test_app_data_into_vec() {
        let app_data = AppData::new(Afn::CtrlCmd, 2, Some(vec![3, 4, 5]));
        let vec: Vec<u8> = app_data.into();
        assert_eq!(vec, vec![Afn::CtrlCmd as u8, 2, 0, 3, 4, 5]);
    }

    #[test]
    fn test_app_data_display() {
        let app_data = AppData::new(Afn::Init, 1, Some(vec![1, 2, 3]));
        let display_string = format!("{}", app_data);
        assert!(display_string.contains("afn: 1, fn: 1"));
        assert!(display_string.contains("type: 0, mark: 1"));
        assert!(display_string.contains("data_units: 010203"));
    }

    #[test]
    fn test_app_data_into_iterator() {
        let app_data = AppData::new(Afn::RouteSet, 3, Some(vec![4, 5, 6]));
        let collected: Vec<u8> = app_data.into_iter().collect();
        assert_eq!(collected, vec![Afn::RouteSet as u8, 3, 0, 4, 5, 6]);
    }

    #[test]
    fn test_data_flag() {
        let data_flag = DataFlag::new(2, 3);
        assert_eq!(data_flag.as_fn_num(), 19);

        let data_flag_from_fn = DataFlag::from(19u8);
        assert_eq!(data_flag_from_fn.type_, 2);
        assert_eq!(data_flag_from_fn.mark, 3);

        let data_flag_from_slice = DataFlag::from([3u8, 2u8].as_slice());
        assert_eq!(data_flag_from_slice.type_, 2);
        assert_eq!(data_flag_from_slice.mark, 3);
    }

    #[test]
    fn test_app_data_with_empty_data_units() {
        let app_data = AppData::new(Afn::Answer, 0, None);
        assert_eq!(app_data.data_length(), 0);
        assert_eq!(app_data.length(), AFN_SIZE + DATA_FLAG_SIZE);
    }

    #[test]
    fn test_app_data_with_large_data_units() {
        let large_data = vec![0; 1000];
        let app_data = AppData::new(Afn::FileTransfer, 1, Some(large_data.clone()));
        assert_eq!(app_data.data_length(), 1000);
        assert_eq!(app_data.length(), AFN_SIZE + DATA_FLAG_SIZE + 1000);
    }

    #[test]
    fn test_app_data_try_from_boundary() {
        let minimal_data = vec![Afn::Debug as u8, 0, 0];
        let app_data = AppData::try_from(minimal_data.as_slice()).unwrap();
        assert_eq!(app_data.afn(), Afn::Debug);
        assert_eq!(app_data.fn_num(), 0);
        assert_eq!(app_data.data_units, None);

        let barely_invalid_data = vec![Afn::Debug as u8, 0];
        assert!(AppData::try_from(barely_invalid_data.as_slice()).is_err());
    }

    #[test]
    fn test_app_data_error_cases() {
        let app_data = AppData::new(Afn::Init, 1, Some(vec![1, 2, 3]));

        let err = app_data.check(Afn::CtrlCmd, 1, 3).unwrap_err();
        assert!(matches!(
            err.downcast_ref::<AppDataError>(),
            Some(AppDataError::Afn(_))
        ));

        let err = app_data.check(Afn::Init, 2, 3).unwrap_err();
        assert!(matches!(
            err.downcast_ref::<AppDataError>(),
            Some(AppDataError::FnNum(_))
        ));

        let err = app_data.check(Afn::Init, 1, 4).unwrap_err();
        assert!(matches!(
            err.downcast_ref::<AppDataError>(),
            Some(AppDataError::DataLength(_))
        ));
    }
}

#[cfg(test)]
pub mod tests_common {
    use super::*;
    use crate::protocol::app_data::{Afn, AppData};
    use crate::protocol::Frame;
    use hex;

    pub fn hex_to_bytes(hex_str: &str) -> Vec<u8> {
        hex::decode(hex_str).expect("Invalid hex string")
    }

    pub fn create_frame_from_hex(hex_str: &str) -> Frame {
        let bytes = hex_to_bytes(hex_str);
        Frame::try_from(bytes.as_slice()).expect("Failed to create frame from hex")
    }

    pub fn test_frame_conversion(hex_str: &str) {
        let frame = create_frame_from_hex(hex_str);
        let reconstructed_hex = hex::encode(frame.to_bytes());
        assert_eq!(hex_str.to_lowercase(), reconstructed_hex);
    }
}
