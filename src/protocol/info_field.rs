use anyhow::ensure;
use std::convert::TryFrom;
use std::fmt;

use crate::Result;

pub const INFO_FIELD_SIZE: usize = 6;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum InfoFieldType {
    Down,
    Up,
}

#[derive(Debug, Clone, Default)]
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

#[derive(Debug, Clone, Default)]
pub struct InfoFieldUp {
    pub route_flag: u8,
    pub comm_model_mark: u8,
    pub relay_level: u8,
    pub channel_flag: u8,
    pub line_mark: u8,
    pub meter_feature: u8,
    pub cmd_signal_quality: u8,
    pub res_signal_quality: u8,
    pub event_mark: u8,
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

    pub fn new(
        info_field_type: InfoFieldType,
        seq: u8,
        relay_level: u8,
        comm_model_mark: u8,
    ) -> Self {
        match info_field_type {
            InfoFieldType::Down => InfoField::Down(InfoFieldDown {
                seq_num: seq,
                comm_model_mark,
                relay_level,
                ..Default::default()
            }),
            InfoFieldType::Up => InfoField::Up(InfoFieldUp {
                seq_num: seq,
                comm_model_mark,
                relay_level,
                ..Default::default()
            }),
        }
    }

    pub fn get_type(&self) -> InfoFieldType {
        match self {
            InfoField::Down(_) => InfoFieldType::Down,
            InfoField::Up(_) => InfoFieldType::Up,
        }
    }

    pub fn relay_level(&self) -> u8 {
        match self {
            InfoField::Down(down) => down.relay_level,
            InfoField::Up(up) => up.relay_level,
        }
    }

    pub fn comm_model_mark(&self) -> u8 {
        match self {
            InfoField::Down(down) => down.comm_model_mark,
            InfoField::Up(up) => up.comm_model_mark,
        }
    }

    pub fn set_comm_model_mark(&mut self, comm_model_mark: u8) {
        match self {
            InfoField::Down(down) => down.comm_model_mark = comm_model_mark,
            InfoField::Up(up) => up.comm_model_mark = comm_model_mark,
        }
    }

    pub fn seq_num(&self) -> u8 {
        match self {
            InfoField::Down(down) => down.seq_num,
            InfoField::Up(up) => up.seq_num,
        }
    }
}

impl TryFrom<&[u8]> for InfoFieldDown {
    type Error = crate::Error;

    fn try_from(bytes: &[u8]) -> Result<Self> {
        ensure!(bytes.len() == INFO_FIELD_SIZE, "Invalid info field size");

        Ok(InfoFieldDown {
            route_flag: bytes[0] & 0x01,
            node_flag: (bytes[0] & 0x02) >> 1,
            comm_model_mark: (bytes[0] & 0x04) >> 2,
            conflict_check: (bytes[0] & 0x08) >> 3,
            relay_level: (bytes[0] & 0xF0) >> 4,
            channel_flag: bytes[1] & 0x0F,
            ecc: (bytes[1] & 0xF0) >> 4,
            answer_bytes: bytes[2],
            speed: u16::from_le_bytes([bytes[3], bytes[4] & 0x7F]),
            speed_unit_flag: (bytes[4] & 0x80) >> 7,
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
            comm_model_mark: (bytes[0] & 0x04) >> 2,
            relay_level: (bytes[0] & 0xF0) >> 4,
            channel_flag: bytes[1] & 0x0F,
            line_mark: bytes[2] & 0x0F,
            meter_feature: (bytes[2] & 0xF0) >> 4,
            cmd_signal_quality: bytes[3] & 0x0F,
            res_signal_quality: (bytes[3] & 0xF0) >> 4,
            event_mark: bytes[4],
            seq_num: bytes[5],
        })
    }
}

impl From<InfoField> for [u8; INFO_FIELD_SIZE] {
    fn from(info: InfoField) -> Self {
        let mut bytes = [0u8; INFO_FIELD_SIZE];
        match info {
            InfoField::Down(down) => {
                bytes[0] = down.route_flag
                    | (down.node_flag << 1)
                    | (down.comm_model_mark << 2)
                    | (down.conflict_check << 3)
                    | (down.relay_level << 4);
                bytes[1] = down.channel_flag | (down.ecc << 4);
                bytes[2] = down.answer_bytes;
                let speed_bytes = down.speed.to_le_bytes();
                bytes[3] = speed_bytes[0];
                bytes[4] = speed_bytes[1] & 0x7F | (down.speed_unit_flag << 7);
                bytes[5] = down.seq_num;
            }
            InfoField::Up(up) => {
                bytes[0] = up.route_flag | (up.comm_model_mark << 2) | (up.relay_level << 4);
                bytes[1] = up.channel_flag;
                bytes[2] = up.line_mark | (up.meter_feature << 4);
                bytes[3] = up.cmd_signal_quality | (up.res_signal_quality << 4);
                bytes[4] = up.event_mark;
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
        writeln!(f, "InfoFieldDown {{ ")?;
        writeln!(f, "  route_flag: {}", self.route_flag)?;
        writeln!(f, "  node_flag: {}", self.node_flag)?;
        writeln!(f, "  comm_model_mark: {}", self.comm_model_mark)?;
        writeln!(f, "  conflict_check: {}", self.conflict_check)?;
        writeln!(f, "  relay_level: {}", self.relay_level)?;
        writeln!(f, "  channel_flag: {}", self.channel_flag)?;
        writeln!(f, "  ecc: {}", self.ecc)?;
        writeln!(f, "  answer_bytes: {}", self.answer_bytes)?;
        writeln!(f, "  speed: {}", self.speed)?;
        writeln!(f, "  speed_unit_flag: {}", self.speed_unit_flag)?;
        writeln!(f, "  seq_num: {}", self.seq_num)?;
        writeln!(f, "}}")
    }
}

impl fmt::Display for InfoFieldUp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "InfoFieldUp {{ ")?;
        writeln!(f, "  route_flag: {}", self.route_flag)?;
        writeln!(f, "  comm_model_mark: {}", self.comm_model_mark)?;
        writeln!(f, "  relay_level: {}", self.relay_level)?;
        writeln!(f, "  channel_flag: {}", self.channel_flag)?;
        writeln!(f, "  line_mark: {}", self.line_mark)?;
        writeln!(f, "  meter_feature: {}", self.meter_feature)?;
        writeln!(f, "  cmd_signal_quality: {}", self.cmd_signal_quality)?;
        writeln!(f, "  res_signal_quality: {}", self.res_signal_quality)?;
        writeln!(f, "  event_mark: {}", self.event_mark)?;
        writeln!(f, "  seq_num: {}", self.seq_num)?;
        writeln!(f, "}}")
    }
}

impl fmt::Display for InfoField {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            InfoField::Down(down) => writeln!(f, "{}", down),
            InfoField::Up(up) => writeln!(f, "{}", up),
        }
    }
}

#[cfg(test)]
mod info_field_tests {
    use super::*;

    #[test]
    fn test_info_field_down_from_bytes() {
        let bytes = [0x1F, 0x2A, 0x03, 0x04, 0x85, 0x06];
        let info_field = InfoField::from_bytes(InfoFieldType::Down, &bytes).unwrap();
        if let InfoField::Down(down) = info_field {
            assert_eq!(down.route_flag, 1);
            assert_eq!(down.node_flag, 1);
            assert_eq!(down.comm_model_mark, 1);
            assert_eq!(down.conflict_check, 1);
            assert_eq!(down.relay_level, 1);
            assert_eq!(down.channel_flag, 10);
            assert_eq!(down.ecc, 2);
            assert_eq!(down.answer_bytes, 3);
            assert_eq!(down.speed, 1284);
            assert_eq!(down.speed_unit_flag, 1);
            assert_eq!(down.seq_num, 6);
        } else {
            panic!("Expected InfoField::Down");
        }
    }

    #[test]
    fn test_info_field_up_from_bytes() {
        let bytes = [0x85, 0x0A, 0x23, 0x45, 0x67, 0x06];
        let info_field = InfoField::from_bytes(InfoFieldType::Up, &bytes).unwrap();
        if let InfoField::Up(up) = info_field {
            assert_eq!(up.route_flag, 1);
            assert_eq!(up.comm_model_mark, 1);
            assert_eq!(up.relay_level, 8);
            assert_eq!(up.channel_flag, 10);
            assert_eq!(up.line_mark, 3);
            assert_eq!(up.meter_feature, 2);
            assert_eq!(up.cmd_signal_quality, 5);
            assert_eq!(up.res_signal_quality, 4);
            assert_eq!(up.event_mark, 0x67);
            assert_eq!(up.seq_num, 6);
        } else {
            panic!("Expected InfoField::Up");
        }
    }

    #[test]
    fn test_info_field_down_to_bytes() {
        let down = InfoFieldDown {
            route_flag: 1,
            node_flag: 1,
            comm_model_mark: 1,
            conflict_check: 1,
            relay_level: 1,
            channel_flag: 10,
            ecc: 2,
            answer_bytes: 3,
            speed: 1284,
            speed_unit_flag: 1,
            seq_num: 6,
        };
        let info_field = InfoField::Down(down);
        let bytes: [u8; INFO_FIELD_SIZE] = info_field.into();
        assert_eq!(bytes, [0x1F, 0x2A, 0x03, 0x04, 0x85, 0x06]);
    }

    #[test]
    fn test_info_field_up_to_bytes() {
        let up = InfoFieldUp {
            route_flag: 1,
            comm_model_mark: 1,
            relay_level: 8,
            channel_flag: 10,
            line_mark: 3,
            meter_feature: 2,
            cmd_signal_quality: 5,
            res_signal_quality: 4,
            event_mark: 0x67,
            seq_num: 6,
        };
        let info_field = InfoField::Up(up);
        let bytes: [u8; INFO_FIELD_SIZE] = info_field.into();
        assert_eq!(bytes, [0x85, 0x0A, 0x23, 0x45, 0x67, 0x06]);
    }

    #[test]
    fn test_new_info_field() {
        let down = InfoField::new(InfoFieldType::Down, 5, 0, 1);
        if let InfoField::Down(down) = down {
            assert_eq!(down.seq_num, 5);
            assert_eq!(down.comm_model_mark, 1);
        } else {
            panic!("Expected InfoField::Down");
        }

        let up = InfoField::new(InfoFieldType::Up, 6, 0, 0);
        if let InfoField::Up(up) = up {
            assert_eq!(up.seq_num, 6);
            assert_eq!(up.comm_model_mark, 0);
        } else {
            panic!("Expected InfoField::Up");
        }
    }

    #[test]
    fn test_get_type() {
        let down = InfoField::new(InfoFieldType::Down, 5, 0, 1);
        assert_eq!(down.get_type(), InfoFieldType::Down);

        let up = InfoField::new(InfoFieldType::Up, 6, 0, 0);
        assert_eq!(up.get_type(), InfoFieldType::Up);
    }

    #[test]
    fn test_info_field_into_iter() {
        let down = InfoFieldDown {
            route_flag: 1,
            node_flag: 1,
            comm_model_mark: 1,
            conflict_check: 1,
            relay_level: 1,
            channel_flag: 10,
            ecc: 2,
            answer_bytes: 3,
            speed: 1284,
            speed_unit_flag: 1,
            seq_num: 6,
        };
        let info_field = InfoField::Down(down);
        let bytes: Vec<u8> = info_field.into_iter().collect();
        assert_eq!(bytes, vec![0x1F, 0x2A, 0x03, 0x04, 0x85, 0x06]);
    }

    #[test]
    fn test_display_info_field_down() {
        let down = InfoFieldDown {
            route_flag: 1,
            node_flag: 1,
            comm_model_mark: 1,
            conflict_check: 1,
            relay_level: 1,
            channel_flag: 10,
            ecc: 2,
            answer_bytes: 3,
            speed: 1284,
            speed_unit_flag: 1,
            seq_num: 6,
        };
        let info_field = InfoField::Down(down);
        let display_string = format!("{}", info_field);
        assert!(display_string.contains("route_flag: 1"));
        assert!(display_string.contains("seq_num: 6"));
        assert!(display_string.contains("speed: 1284"));
    }

    #[test]
    fn test_display_info_field_up() {
        let up = InfoFieldUp {
            route_flag: 1,
            comm_model_mark: 1,
            relay_level: 8,
            channel_flag: 10,
            line_mark: 3,
            meter_feature: 2,
            cmd_signal_quality: 5,
            res_signal_quality: 4,
            event_mark: 0x67,
            seq_num: 6,
        };
        let info_field = InfoField::Up(up);
        let display_string = format!("{}", info_field);
        assert!(display_string.contains("route_flag: 1"));
        assert!(display_string.contains("seq_num: 6"));
        assert!(display_string.contains("event_mark: 103"));
    }
}
