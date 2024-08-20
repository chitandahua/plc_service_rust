use anyhow::ensure;
use std::convert::TryFrom;
use std::fmt::Formatter;
use std::fmt::{self, Display};

use crate::Result;

pub const INFO_FIELD_SIZE: usize = 6;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum InfoFieldType {
    Down,
    Up,
}

#[derive(Debug, Clone)]
pub struct InfoFieldDown {
    pub route_flag: u8,
    pub node_flag: u8,
    pub comm_model_mark: u8,
    pub conflict_check: u8,
    pub relay_level: u8,
    pub channel_flag: u8,
    pub ecc: u8,
    pub answer_bytes: u8,
    pub speed: u16,
    pub speed_unit_flag: u8,
    pub seq_num: u8,
}

#[derive(Debug, Clone)]
pub struct InfoFieldUp {
    pub route_flag: u8,
    pub comm_model_mark: u8,
    pub relay_level: u8,
    pub channel_flag: u8,
    pub line_mark: u8,
    pub meter_feature: u8,
    pub cmd_signal_quality: u8,
    pub res_signal_quality: u8,
    pub seq_num: u8,
}

#[derive(Debug, Clone)]
pub enum InfoField {
    Down(InfoFieldDown),
    Up(InfoFieldUp),
}

impl InfoField {
    pub fn from_bytes(info_field_type: InfoFieldType, bytes: &[u8]) -> Result<Self> {
        match info_field_type {
            InfoFieldType::Down => Ok(InfoField::Down(bytes.try_into()?)),
            InfoFieldType::Up => Ok(InfoField::Up(bytes.try_into()?)),
        }
    }

    pub fn new(info_field_type: InfoFieldType, seq: u8) -> Self {
        todo!()
    }

    pub fn get_type(&self) -> InfoFieldType {
        match self {
            InfoField::Down(_) => InfoFieldType::Down,
            InfoField::Up(_) => InfoFieldType::Up,
        }
    }
}

impl TryFrom<&[u8]> for InfoFieldDown {
    type Error = crate::Error;

    fn try_from(bytes: &[u8]) -> Result<Self> {
        ensure!(bytes.len() == INFO_FIELD_SIZE, "Invalid info field size");

        Ok(InfoFieldDown {
            route_flag: bytes[0] & 0x01,
            node_flag: bytes[0] & 0x02,
            comm_model_mark: bytes[0] & 0x04,
            conflict_check: bytes[0] & 0x08,
            relay_level: (bytes[0] & 0xF0) >> 4,
            channel_flag: bytes[1] & 0x0F,
            ecc: (bytes[1] & 0xF0) >> 4,
            answer_bytes: bytes[2],
            speed: u16::from_le_bytes([bytes[3], bytes[4] & 0x7F]),
            speed_unit_flag: bytes[4] & 0x80,
            seq_num: bytes[5],
        })
    }
}

impl TryFrom<&[u8]> for InfoFieldUp {
    type Error = crate::Error;

    fn try_from(bytes: &[u8]) -> Result<Self> {
        ensure!(bytes.len() == INFO_FIELD_SIZE, "Invalid info field size");

        Ok(InfoFieldUp {
            route_flag: bytes[0] & 0x01,
            comm_model_mark: bytes[0] & 0x04,
            relay_level: (bytes[0] & 0xF0) >> 4,
            channel_flag: bytes[1] & 0x0F,
            line_mark: bytes[2] & 0x0F,
            meter_feature: (bytes[2] & 0xF0) >> 4,
            cmd_signal_quality: bytes[3] & 0x0F,
            res_signal_quality: (bytes[3] & 0xF0) >> 4,
            seq_num: bytes[5],
        })
    }
}

impl From<InfoField> for [u8; INFO_FIELD_SIZE] {
    fn from(info: InfoField) -> Self {
        let mut bytes = [0u8; INFO_FIELD_SIZE];
        match info {
            InfoField::Down(down) => {
                bytes[0] = (down.route_flag as u8)
                    | ((down.node_flag as u8) << 1)
                    | ((down.comm_model_mark as u8) << 2)
                    | ((down.conflict_check as u8) << 3)
                    | (down.relay_level << 4);
                bytes[1] = down.channel_flag | (down.ecc << 4);
                bytes[2] = down.answer_bytes;
                let speed_bytes = down.speed.to_le_bytes();
                bytes[3] = speed_bytes[0];
                bytes[4] = speed_bytes[1] & 0x7F | ((down.speed_unit_flag as u8) << 7);
                bytes[5] = down.seq_num;
            }
            InfoField::Up(up) => {
                bytes[0] = 0x80 // Set the high bit to indicate Up type
                    | (up.route_flag as u8)
                    | ((up.comm_model_mark as u8) << 2)
                    | (up.relay_level << 4);
                bytes[1] = up.channel_flag;
                bytes[2] = up.line_mark | (up.meter_feature << 4);
                bytes[3] = up.cmd_signal_quality | (up.res_signal_quality << 4);
                // bytes[4] is reserved and remains 0
                bytes[5] = up.seq_num;
            }
        }
        bytes
    }
}

impl IntoIterator for InfoField {
    type Item = u8;
    type IntoIter = std::array::IntoIter<Self::Item, INFO_FIELD_SIZE>;

    fn into_iter(self) -> Self::IntoIter {
        //self.into().into_iter()
        Into::<[Self::Item; INFO_FIELD_SIZE]>::into(self).into_iter()
    }
}

impl fmt::Display for InfoFieldDown {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "InfoFieldDown {{ \n")?;
        write!(f, "  route_flag: {}\n", self.route_flag)?;
        write!(f, "  node_flag: {}\n", self.node_flag)?;
        write!(f, "  comm_model_mark: {}\n", self.comm_model_mark)?;
        write!(f, "  conflict_check: {}\n", self.conflict_check)?;
        write!(f, "  relay_level: {}\n", self.relay_level)?;
        write!(f, "  channel_flag: {}\n", self.channel_flag)?;
        write!(f, "  ecc: {}\n", self.ecc)?;
        write!(f, "  answer_bytes: {}\n", self.answer_bytes)?;
        write!(f, "  speed: {}\n", self.speed)?;
        write!(f, "  speed_unit_flag: {}\n", self.speed_unit_flag)?;
        write!(f, "  seq_num: {}\n", self.seq_num)?;
        write!(f, "}}")
    }
}

impl fmt::Display for InfoFieldUp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "InfoFieldUp {{ \n")?;
        write!(f, "  route_flag: {}\n", self.route_flag)?;
        write!(f, "  comm_model_mark: {}\n", self.comm_model_mark)?;
        write!(f, "  relay_level: {}\n", self.relay_level)?;
        write!(f, "  channel_flag: {}\n", self.channel_flag)?;
        write!(f, "  line_mark: {}\n", self.line_mark)?;
        write!(f, "  meter_feature: {}\n", self.meter_feature)?;
        write!(f, "  cmd_signal_quality: {}\n", self.cmd_signal_quality)?;
        write!(f, "  res_signal_quality: {}\n", self.res_signal_quality)?;
        write!(f, "  seq_num: {}\n", self.seq_num)?;
        write!(f, "}}")
    }
}

impl fmt::Display for InfoField {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            InfoField::Down(down) => write!(f, "{}", down),
            InfoField::Up(up) => write!(f, "{}", up),
        }
    }
}
