use num_enum::TryFromPrimitive;

use crate::protocol::app_data::{Address, Afn};
use crate::protocol::AppData;

use crate::service;

// AFN 11H
#[derive(Debug, TryFromPrimitive)]
#[repr(u8)]
pub enum RouteSet {
    AddNode = 1,
    DelNode = 2,
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

    pub fn to_node_info(&self) -> service::NodeInfo {
        service::NodeInfo::new(
            self.src_addr.to_string(),
            format!("{:02x}", self.protocol_type),
        )
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
