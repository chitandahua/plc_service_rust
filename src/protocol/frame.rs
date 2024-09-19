use anyhow::ensure;
use num_enum::{IntoPrimitive, TryFromPrimitive};
use std::fmt;
use std::fmt::Formatter;
use std::io::Cursor;
use std::sync::atomic::{AtomicU8, Ordering};
use strum_macros::EnumString;
use thiserror::Error;

use crate::protocol::app_data::{Afn, AnswerFn};
use crate::protocol::{info_field, AddressField, AppData, UserData};
use crate::Result;

#[derive(Debug, Clone)]
struct Header {
    header: u8,
    length: u16,
}

const HEADER: u8 = 0x68;

impl Header {
    fn new(length: u16) -> Self {
        Header {
            header: HEADER,
            length,
        }
    }
}

impl From<&[u8]> for Header {
    fn from(bytes: &[u8]) -> Self {
        Header {
            header: bytes[0],
            length: u16::from_le_bytes([bytes[1], bytes[2]]),
        }
    }
}

impl From<Header> for Vec<u8> {
    fn from(header: Header) -> Self {
        let mut data = vec![header.header];
        // length高位放后
        data.extend(header.length.to_le_bytes());
        data
        //vec![header.header, header.length as u8, ((header.length >> 8) as u8) & 0xff]
    }
}

impl IntoIterator for Header {
    type Item = u8;
    type IntoIter = std::vec::IntoIter<u8>;
    fn into_iter(self) -> Self::IntoIter {
        //self.into().into_iter()
        Into::<Vec<u8>>::into(self).into_iter()
    }
}

// ctrl field
#[derive(
    Debug, Clone, PartialEq, EnumString, IntoPrimitive, TryFromPrimitive, strum_macros::Display,
)]
#[strum(serialize_all = "lowercase")]
#[repr(u8)]
enum Dir {
    Down = 0,
    Up = 1,
}

#[derive(
    Debug, Clone, PartialEq, EnumString, IntoPrimitive, TryFromPrimitive, strum_macros::Display,
)]
#[strum(serialize_all = "lowercase")]
#[repr(u8)]
enum Prm {
    Slave = 0,
    Master = 1,
}

#[derive(Debug, Clone, EnumString, IntoPrimitive, TryFromPrimitive, strum_macros::Display)]
#[repr(u8)]
enum Comm {
    #[strum(serialize = "Centralize")]
    Centralize = 1,
    #[strum(serialize = "Decentralize")]
    Decentralize = 2,
    #[strum(serialize = "Hplc")]
    Hplc = 3,
    #[strum(serialize = "LowerPower")]
    LowerPower = 10,
    #[strum(serialize = "Ethernet")]
    Ethernet = 20,
}

#[derive(Debug, Clone)]
struct CtrlField {
    dir: Dir,
    prm: Prm,
    comm: Comm,
}

impl Default for CtrlField {
    fn default() -> Self {
        CtrlField {
            dir: Dir::Down,
            prm: Prm::Master,
            comm: Comm::Hplc,
        }
    }
}

impl CtrlField {
    fn new(is_response: bool) -> Self {
        CtrlField {
            dir: Dir::Down,
            prm: if is_response { Prm::Slave } else { Prm::Master },
            comm: Comm::Hplc,
        }
    }

    fn get_info_field_type(&self) -> info_field::InfoFieldType {
        match self.dir {
            Dir::Down => info_field::InfoFieldType::Down,
            Dir::Up => info_field::InfoFieldType::Up,
        }
    }
}

impl From<CtrlField> for u8 {
    fn from(ctrl_field: CtrlField) -> Self {
        // 7: dir, 6: prm, 5-0: comm
        (Into::<u8>::into(ctrl_field.dir) << 7)
            | (Into::<u8>::into(ctrl_field.prm) << 6)
            | Into::<u8>::into(ctrl_field.comm)
    }
}

impl TryFrom<u8> for CtrlField {
    type Error = crate::Error;
    fn try_from(ctrl_field: u8) -> Result<Self> {
        Ok(CtrlField {
            dir: (ctrl_field >> 7).try_into()?,
            prm: ((ctrl_field >> 6) & 1).try_into()?,
            comm: (ctrl_field & 0x3f).try_into()?,
        })
    }
}

impl fmt::Display for CtrlField {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "dir: {}", self.dir)?;
        writeln!(f, "prm: {}", self.prm)?;
        writeln!(f, "comm: {}", self.comm)
    }
}

impl IntoIterator for CtrlField {
    type Item = u8;
    type IntoIter = std::iter::Once<u8>;
    fn into_iter(self) -> Self::IntoIter {
        std::iter::once(self.into())
    }
}

// checksum
#[derive(Debug, Default, Clone)]
struct Checksum {
    checksum: u8,
}

impl Checksum {
    fn new(checksum: u8) -> Self {
        Checksum { checksum }
    }
}

// tail
const TAIL: u8 = 0x16;

#[derive(Debug, Clone)]
struct Tail {
    tail: u8,
}

impl Default for Tail {
    fn default() -> Self {
        Tail { tail: TAIL }
    }
}

// frame
const HEADER_SIZE: usize = 3;
const CTRL_FIELD_SIZE: usize = 1;
const CHECK_SUM_SIZE: usize = 1;
const TAIL_SIZE: usize = 1;
const FRAME_SIZE: usize = HEADER_SIZE + CTRL_FIELD_SIZE + CHECK_SUM_SIZE + TAIL_SIZE;
const LAST_SIZE: usize = CHECK_SUM_SIZE + TAIL_SIZE;

const HEADER_OFFSET: usize = 0;
const CTRL_FIELD_OFFSET: usize = HEADER_OFFSET + HEADER_SIZE;
const USER_DATA_OFFSET: usize = CTRL_FIELD_OFFSET + CTRL_FIELD_SIZE;

#[derive(Debug, Clone)]
pub struct Frame {
    header: Header,
    ctrl_field: CtrlField,
    user_data: UserData,
    checksum: Checksum,
    tail: Tail,
}

fn calc_checksum(bytes: &[u8]) -> u8 {
    let mut checksum = 0u8;
    bytes
        .iter()
        .for_each(|b| checksum = checksum.wrapping_add(*b));
    checksum
}

static SEQ: AtomicU8 = AtomicU8::new(0);

impl Frame {
    fn new(
        is_response: bool,
        seq: u8,
        address_field: Option<AddressField>,
        app_data: AppData,
    ) -> Self {
        let user_data = UserData::new(seq, address_field, app_data);
        let mut frame = Frame {
            header: Header::new((FRAME_SIZE + user_data.length()) as u16),
            ctrl_field: CtrlField::new(is_response),
            user_data,
            //..Default::default() // TODO 需要为Frame以及剩余字段impl Default
            checksum: Default::default(),
            tail: Default::default(),
        };
        let bytes = frame.to_bytes();
        frame.checksum = Checksum::new(calc_checksum(
            &bytes[CTRL_FIELD_OFFSET..bytes.len() - LAST_SIZE],
        ));
        frame
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        self.clone().into()
    }

    pub fn new_request(address_field: Option<AddressField>, app_data: impl Into<AppData>) -> Self {
        Frame::new(
            false,
            SEQ.fetch_add(1, Ordering::Relaxed),
            address_field,
            app_data.into(),
        )
    }

    pub fn new_response(
        seq: u8,
        address_field: Option<AddressField>,
        app_data: impl Into<AppData>,
    ) -> Self {
        Frame::new(true, seq, address_field, app_data.into())
    }

    pub fn into_app_data(self) -> AppData {
        self.user_data.into_app_data()
    }

    pub fn is_confirm(&self) -> bool {
        self.user_data.app_data.afn() == Afn::Answer
            && self.user_data.app_data.fn_num() == AnswerFn::Confirm as u8
    }

    pub fn is_deny(&self) -> bool {
        self.user_data.app_data.afn() == Afn::Answer
            && self.user_data.app_data.fn_num() == AnswerFn::Deny as u8
    }

    pub fn is_slave_report(&self) -> bool {
        self.ctrl_field.dir == Dir::Up && self.ctrl_field.prm == Prm::Master
    }

    pub fn is_master_response(&self) -> bool {
        self.ctrl_field.dir == Dir::Down && self.ctrl_field.prm == Prm::Slave
    }

    pub fn parse(src: &mut Cursor<&[u8]>) -> Result<Option<Self>> {
        let end = src.get_ref().len();

        // header 不匹配的则直接移除
        while src.position() < end as u64 && src.get_ref()[src.position() as usize] != HEADER {
            src.set_position(src.position() + 1);
        }

        // 判断长度
        let start = src.position() as usize;
        if end - start < HEADER_SIZE {
            return Ok(None);
        }
        let length = u16::from_le_bytes(src.get_ref()[1..HEADER_SIZE].try_into()?) as usize;
        if end - start < length {
            return Ok(None);
        }

        src.set_position(src.position() + length as u64);
        Ok(Some(src.get_ref()[start..start + length].try_into()?))
    }

    pub fn get_seq(&self) -> u8 {
        self.user_data.get_seq()
    }

    pub fn afn(&self) -> Afn {
        self.user_data.app_data.afn()
    }

    pub fn fn_num(&self) -> u8 {
        self.user_data.app_data.fn_num()
    }

    pub fn match_req(&self, seq: u8) -> bool {
        self.user_data.get_seq() == seq
    }

    pub fn to_hex_string(&self) -> String {
        //hex::encode(self.to_bytes())
        self.to_bytes()
            .iter()
            .map(|b| format!("{:02X}", b))
            .collect::<Vec<String>>()
            .join(" ")
    }
}

#[derive(Error, Debug, PartialEq, EnumString)]
pub(crate) enum FrameError {
    #[error("length {0} error")]
    Length(usize),
    #[error("header {0} error")]
    Header(u8),
    #[error("checksum {checksum:02x} error, expected {expected:02x}")]
    Checksum { checksum: u8, expected: u8 },
    #[error("tail {0} error")]
    Tail(u8),
}

impl TryFrom<&[u8]> for Frame {
    type Error = anyhow::Error;
    fn try_from(bytes: &[u8]) -> Result<Self> {
        ensure!(bytes.len() >= FRAME_SIZE, FrameError::Length(bytes.len()));
        // header
        let header = Header::from(&bytes[HEADER_OFFSET..HEADER_OFFSET + HEADER_SIZE]);
        ensure!(header.header == HEADER, FrameError::Header(header.header));
        ensure!(
            header.length as usize <= bytes.len(),
            FrameError::Length(bytes.len())
        );
        // ctrl field
        let ctrl_field: CtrlField = bytes[CTRL_FIELD_OFFSET].try_into()?;
        // user data
        let user_data = UserData::from_bytes(
            ctrl_field.get_info_field_type(),
            &bytes[USER_DATA_OFFSET..bytes.len() - LAST_SIZE],
        )?;
        // checksum
        let checksum = bytes[bytes.len() - LAST_SIZE];
        let calc_checksum = calc_checksum(&bytes[CTRL_FIELD_OFFSET..bytes.len() - LAST_SIZE]);
        ensure!(
            checksum == calc_checksum,
            FrameError::Checksum {
                expected: calc_checksum,
                checksum
            }
        );
        // tail
        let tail = bytes[bytes.len() - TAIL_SIZE];
        ensure!(tail == TAIL, FrameError::Tail(tail));

        Ok(Frame {
            header,
            ctrl_field,
            user_data,
            checksum: Checksum { checksum },
            tail: Tail { tail },
        })
    }
}

impl From<Frame> for Vec<u8> {
    fn from(frame: Frame) -> Self {
        let mut bytes = Vec::new();
        bytes.extend(frame.header);
        bytes.extend(frame.ctrl_field);
        bytes.extend(frame.user_data);
        bytes.push(frame.checksum.checksum);
        bytes.push(frame.tail.tail);
        bytes
    }
}

impl fmt::Display for Frame {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "ctrl_field:")?;
        writeln!(f, "{}", self.ctrl_field)?;
        writeln!(f, "user_data:")?;
        writeln!(f, "{}", self.user_data)?;
        writeln!(f, "checksum: {}", self.checksum.checksum)
    }
}

#[cfg(test)]
mod tests {
    use info_field::InfoFieldType;

    use super::*;
    use crate::protocol::app_data::{tests_common, Afn};
    use crate::protocol::Address;

    fn create_dummy_app_data() -> AppData {
        AppData::new(Afn::QueryData, 1, Some(vec![0x01, 0x02, 0x03]))
    }

    #[test]
    fn test_frame_new_request() {
        let app_data = create_dummy_app_data();
        let frame = Frame::new_request(None, app_data.clone());

        assert_eq!(frame.ctrl_field.dir, Dir::Down);
        assert_eq!(frame.ctrl_field.prm, Prm::Master);
        assert_eq!(frame.user_data.app_data, app_data);
        assert_eq!(frame.tail.tail, TAIL);
    }

    #[test]
    fn test_frame_new_response() {
        let app_data = create_dummy_app_data();
        let frame = Frame::new_response(5, None, app_data.clone());

        assert_eq!(frame.ctrl_field.dir, Dir::Down);
        assert_eq!(frame.ctrl_field.prm, Prm::Slave);
        assert_eq!(frame.user_data.app_data, app_data);
        assert_eq!(frame.get_seq(), 5);
        assert_eq!(frame.tail.tail, TAIL);
    }

    #[test]
    fn test_frame_to_bytes_and_back() {
        let original_frame = Frame::new_request(None, create_dummy_app_data());
        let bytes = original_frame.to_bytes();
        let reconstructed_frame = Frame::try_from(bytes.as_slice()).unwrap();

        assert_eq!(
            original_frame.user_data.app_data,
            reconstructed_frame.user_data.app_data
        );
        assert_eq!(
            original_frame.ctrl_field.dir,
            reconstructed_frame.ctrl_field.dir
        );
        assert_eq!(
            original_frame.ctrl_field.prm,
            reconstructed_frame.ctrl_field.prm
        );
    }

    #[test]
    fn test_frame_checksum() {
        let frame = tests_common::create_frame_from_hex("680f00430000000000000102004616");
        assert_eq!(frame.checksum.checksum, 0x46);
    }

    #[test]
    fn test_frame_parse_valid() {
        let frame = Frame::new_request(None, create_dummy_app_data());
        let bytes = frame.to_bytes();
        let mut cursor = Cursor::new(bytes.as_slice());
        //println!("bytes: {}", hex::encode(&bytes));
        let parsed_frame = Frame::parse(&mut cursor).unwrap().unwrap();
        assert_eq!(frame.user_data.app_data, parsed_frame.user_data.app_data);
    }

    #[test]
    fn test_frame_parse_invalid_header() {
        let mut invalid_bytes = vec![0x00]; // Invalid header
        invalid_bytes.extend(Frame::new_request(None, create_dummy_app_data()).to_bytes());
        let mut cursor = Cursor::new(invalid_bytes.as_slice());
        let result = Frame::parse(&mut cursor);
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn test_frame_try_from_invalid_length() {
        let short_bytes = vec![0x68, 0x03, 0x00]; // Too short
        let result = Frame::try_from(short_bytes.as_slice());
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err().downcast_ref::<FrameError>(),
            Some(FrameError::Length(_))
        ));
    }

    #[test]
    fn test_frame_try_from_invalid_checksum() {
        let mut frame = Frame::new_request(None, create_dummy_app_data());
        frame.checksum.checksum = 0xFF; // Incorrect checksum
        let bytes = frame.to_bytes();
        let result = Frame::try_from(bytes.as_slice());
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err().downcast_ref::<FrameError>(),
            Some(FrameError::Checksum { .. })
        ));
    }

    #[test]
    fn test_frame_try_from_invalid_tail() {
        let mut frame = Frame::new_request(None, create_dummy_app_data());
        frame.tail.tail = 0x00; // Incorrect tail
        let bytes = frame.to_bytes();
        let result = Frame::try_from(bytes.as_slice());
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err().downcast_ref::<FrameError>(),
            Some(FrameError::Tail(_))
        ));
    }

    #[test]
    fn test_frame_sequence_number() {
        let frame1 = Frame::new_request(None, create_dummy_app_data());
        let frame2 = Frame::new_request(None, create_dummy_app_data());
        assert_ne!(frame1.get_seq(), frame2.get_seq());
    }

    #[test]
    fn test_frame_is_confirm() {
        let app_data = AppData::new(Afn::Answer, AnswerFn::Confirm as u8, Some(vec![]));
        let frame = Frame::new_response(0, None, app_data);
        assert!(frame.is_confirm());
    }

    #[test]
    fn test_frame_is_deny() {
        let app_data = AppData::new(Afn::Answer, AnswerFn::Deny as u8, Some(vec![]));
        let frame = Frame::new_response(0, None, app_data);
        assert!(frame.is_deny());
    }

    #[test]
    fn test_frame_is_slave_report() {
        let mut frame = Frame::new_request(None, create_dummy_app_data());
        frame.ctrl_field.dir = Dir::Up;
        frame.ctrl_field.prm = Prm::Master;
        assert!(frame.is_slave_report());
    }

    #[test]
    fn test_frame_is_master_response() {
        let frame = Frame::new_response(0, None, create_dummy_app_data());
        assert!(frame.is_master_response());
    }

    #[test]
    fn test_afn_00h_f1() {
        let hex_str = "681500030000000000000001000000000006000a16";
        let frame = tests_common::create_frame_from_hex(hex_str);

        let Frame {
            ctrl_field,
            user_data,
            checksum,
            ..
        } = frame;

        // ctrl_field
        assert!(matches!(ctrl_field.dir, Dir::Down));
        assert!(matches!(ctrl_field.prm, Prm::Slave));
        assert!(matches!(ctrl_field.comm, Comm::Hplc));

        // user_data
        assert_eq!(user_data.info_field.get_type(), InfoFieldType::Down);
        assert_eq!(user_data.info_field.comm_model_mark(), 0);
        assert_eq!(user_data.info_field.seq_num(), 0);
        assert!(user_data.address_field.is_none());

        assert_eq!(user_data.app_data.afn(), Afn::Answer);
        assert_eq!(user_data.app_data.fn_num(), AnswerFn::Confirm as u8);
        // checksum
        assert_eq!(checksum.checksum, 0x0a);
    }

    #[test]
    fn _test_afn_13h_f1() {
        //use crate::protocol::app_data::DataForward;

        let hex_str = "68390043040000000000ab8967563412ab8967564321130100020002ab8967563413ab89675634140e6812345678901268010243c3ac16bb16";
        let frame = tests_common::create_frame_from_hex(hex_str);

        let Frame {
            ctrl_field,
            user_data,
            checksum,
            ..
        } = frame;

        // ctrl_field
        assert!(matches!(ctrl_field.dir, Dir::Down));
        assert!(matches!(ctrl_field.prm, Prm::Master));
        assert!(matches!(ctrl_field.comm, Comm::Hplc));

        // user_data
        assert_eq!(user_data.info_field.get_type(), InfoFieldType::Down);
        assert_eq!(user_data.info_field.comm_model_mark(), 1);
        assert_eq!(user_data.info_field.seq_num(), 0);

        let address_field = user_data.address_field.unwrap();
        assert_eq!(
            address_field.src_address,
            Address::new([0x12, 0x34, 0x56, 0x67, 0x89, 0xAB])
        );
        assert_eq!(
            address_field.dst_address,
            Address::new([0x21, 0x43, 0x56, 0x67, 0x89, 0xAB])
        );
        assert!(address_field.relay_address.is_none());

        assert_eq!(user_data.app_data.afn(), Afn::RouteDataForward);
        //assert_eq!(user_data.app_data.fn_num(), DataForward::MonitorNode as u8);
        // checksum
        assert_eq!(checksum.checksum, 0xbb);
    }
}
