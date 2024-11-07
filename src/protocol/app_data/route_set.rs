use crate::protocol::user_data::dec_to_hex;
use num_enum::TryFromPrimitive;

use crate::protocol::app_data::{Address, Afn};
use crate::protocol::AppData;

use chrono::{Datelike, Timelike};

// AFN 11H
#[derive(Debug, TryFromPrimitive)]
#[repr(u8)]
pub enum RouteSet {
    AddNode = 1,
    DelNode = 2,
    ActiveNodeRegister = 5,
    StopNodeRegister = 6,
}

#[derive(Debug, PartialEq)]
pub struct NodeInfo {
    src_addr: Address,
    protocol_type: u8,
}

impl NodeInfo {
    pub fn new(src_addr: Address, protocol_type: u8) -> Self {
        Self {
            src_addr,
            protocol_type,
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct AddNodeRequest {
    node_infos: Vec<NodeInfo>,
}

impl AddNodeRequest {
    pub fn new(node_infos: Vec<NodeInfo>) -> Self {
        Self { node_infos }
    }
}

impl From<AddNodeRequest> for AppData {
    fn from(request: AddNodeRequest) -> Self {
        let mut data = Vec::new();
        data.push(request.node_infos.len() as u8);
        for node in request.node_infos {
            data.extend(node.src_addr);
            data.push(node.protocol_type);
        }
        AppData::new(Afn::RouteSet, RouteSet::AddNode as u8, Some(data))
    }
}

#[derive(Debug, PartialEq)]
pub struct DelNodeRequest {
    node_addrs: Vec<Address>,
}

impl DelNodeRequest {
    pub fn new(node_addrs: Vec<Address>) -> Self {
        Self { node_addrs }
    }
}

impl From<DelNodeRequest> for AppData {
    fn from(request: DelNodeRequest) -> Self {
        let mut data = Vec::new();
        data.push(request.node_addrs.len() as u8);
        for addr in request.node_addrs {
            data.extend(addr);
        }
        AppData::new(Afn::RouteSet, RouteSet::DelNode as u8, Some(data))
    }
}

pub struct ActiveNodeRegisterRequest {
    start_time: chrono::NaiveDateTime,
    // 持续时间
    duration: u16,
    retry_count: u8,
    // 随机等待时间片个数
    random_wait_count: u8,
}

impl ActiveNodeRegisterRequest {
    pub fn new(
        start_time: chrono::NaiveDateTime,
        duration: u16,
        retry_count: u8,
        random_wait_count: u8,
    ) -> Self {
        Self {
            start_time,
            duration,
            retry_count,
            random_wait_count,
        }
    }
}

impl From<ActiveNodeRegisterRequest> for AppData {
    fn from(request: ActiveNodeRegisterRequest) -> Self {
        let mut data = vec![
            dec_to_hex(request.start_time.second() as u8),
            dec_to_hex(request.start_time.minute() as u8),
            dec_to_hex(request.start_time.hour() as u8),
            dec_to_hex(request.start_time.day() as u8),
            dec_to_hex(request.start_time.month() as u8),
            dec_to_hex((request.start_time.year() % 100) as u8),
        ];

        data.extend(request.duration.to_le_bytes());
        data.push(request.retry_count);
        data.push(request.random_wait_count);
        AppData::new(
            Afn::RouteSet,
            RouteSet::ActiveNodeRegister as u8,
            Some(data),
        )
    }
}

pub struct StopNodeRegisterRequest;

impl From<StopNodeRegisterRequest> for AppData {
    fn from(_: StopNodeRegisterRequest) -> Self {
        AppData::new(Afn::RouteSet, RouteSet::StopNodeRegister as u8, None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::app_data::*;

    #[test]
    fn test_add_node_request() {
        let frame = tests_common::create_frame_from_hex(
            "6825004300000000000011010003ab896756341201ab896756341302ab8967563414030616",
        );

        let add_node_request = AddNodeRequest {
            node_infos: vec![
                NodeInfo {
                    src_addr: Address::new([0x12, 0x34, 0x56, 0x67, 0x89, 0xab]),
                    protocol_type: 0x01,
                },
                NodeInfo {
                    src_addr: Address::new([0x13, 0x34, 0x56, 0x67, 0x89, 0xab]),
                    protocol_type: 0x02,
                },
                NodeInfo {
                    src_addr: Address::new([0x14, 0x34, 0x56, 0x67, 0x89, 0xab]),
                    protocol_type: 0x03,
                },
            ],
        };

        assert_eq!(frame.into_app_data(), add_node_request.into());
    }

    #[test]
    fn test_del_node_request() {
        let frame = tests_common::create_frame_from_hex(
            "6822004300000000000011020003ab8967563412ab8967563413ab89675634140116",
        );

        let del_node_request = DelNodeRequest {
            node_addrs: vec![
                Address::new([0x12, 0x34, 0x56, 0x67, 0x89, 0xab]),
                Address::new([0x13, 0x34, 0x56, 0x67, 0x89, 0xab]),
                Address::new([0x14, 0x34, 0x56, 0x67, 0x89, 0xab]),
            ],
        };

        assert_eq!(frame.into_app_data(), del_node_request.into());
    }
}
