use thiserror::Error;
use std::fmt::Formatter;
use std::fmt;
use anyhow::ensure;
use std::sync::atomic::{AtomicU8, Ordering};
use strum_macros::{EnumString, ToString};
use num_enum::{IntoPrimitive, TryFromPrimitive};

use crate::protocol::{AppData, UserData};
use crate::protocol::app_data::{Afn, AnswerFn};

#[derive(Debug)]
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
#[derive(Debug, Clone, PartialEq, EnumString, ToString, IntoPrimitive, TryFromPrimitive, strum_macros::Display)]
#[strum(serialize_all = "lowercase")]
#[repr(u8)]
enum Dir {
    Down = 0,
    Up = 1,
}

#[derive(Debug, Clone, PartialEq, EnumString, ToString, IntoPrimitive, TryFromPrimitive, strum_macros::Display)]
#[strum(serialize_all = "lowercase")]
#[repr(u8)]
enum Prm {
    Slave = 0,
    Master = 1,
}

#[derive(Debug, Clone, EnumString, ToString, IntoPrimitive, TryFromPrimitive, strum_macros::Display)]
#[repr(u8)]
enum Comm {
    #[strum(serialize = "Centralize")]
    Centralize = 1,
    #[strum(serialize = "Decentralize")]
    Decentralize = 2,
    #[strum(serialize = "HPLC")]
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
}

impl From<CtrlField> for u8 {
    fn from(ctrl_field: CtrlField) -> Self {
        (ctrl_field.dir.into() << 7) | (ctrl_field.prm.into() << 6) | ctrl_field.comm.into()
    }
}

impl TryFrom<u8> for CtrlField {
    type Error = crate::Error;
    fn try_from(ctrl_field: u8) -> Result<Self, Self::Error> {
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
#[derive(Debug, Default)]
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

#[derive(Debug)]
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

#[derive(Debug)]
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
    fn checksum(&self) -> u8 {
        let mut bytes = Vec::new();
        bytes.push(self.ctrl_field.clone().into());
        bytes.extend(self.user_data.clone()); // TODO 
        calc_checksum(&bytes)
    }

    fn new(is_response: bool, seq: u8, app_data: AppData) -> Self {
        let user_data = UserData::new(seq, app_data);
        let mut frame = Frame {
            header: Header::new((FRAME_SIZE + user_data.length()) as u16),
            ctrl_field: CtrlField::new(is_response),
            user_data,
            ..Default::default()
        };
        frame.checksum = Checksum::new(frame.checksum());
        frame
    }

    pub fn new_request(app_data: AppData) -> Self {
        Frame::new(false, SEQ.fetch_add(1, Ordering::Relaxed), app_data)
    }

    pub fn new_response(seq: u8, app_data: AppData) -> Self {
        Frame::new(true, seq, app_data)
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
}

#[derive(Error, Debug, PartialEq, EnumString, ToString, Display)]
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
    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        ensure!(
            bytes.len() >= FRAME_SIZE,
            FrameError::Length(bytes.len())
        );
        // header
        let header = Header::from(&bytes[HEADER_OFFSET..HEADER_OFFSET + HEADER_SIZE]);
        ensure!(header.header == HEADER, FrameError::Header(header.header));
        ensure!(
            header.length as usize <= bytes.len(),
            FrameError::Length(bytes.len())
        );
        // ctrl field
        let ctrl_field = bytes[CTRL_FIELD_OFFSET].try_into()?;
        // user data
        let user_data = UserData::try_from(&bytes[USER_DATA_OFFSET..bytes.len() - LAST_SIZE])?;
        // checksum
        let checksum = bytes[bytes.len() - LAST_SIZE];
        let calc_checksum = calc_checksum(&bytes[CTRL_FIELD_OFFSET..bytes.len() - LAST_SIZE]);
        ensure!(
            checksum == calc_checksum,
            FrameError::Checksum{expected: calc_checksum, checksum}
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
