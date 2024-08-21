use std::fmt::Formatter;
use std::fmt::{self, Display};

use crate::protocol::app_data::{Address, Afn};
use crate::protocol::AppData;

pub enum RouteSet {
    AddNode = 1,
    DelNode = 2,
}

pub struct NodeInfo {
    src_addr: Address,
    protocol_type: u8,
}

pub struct AddNodeRequest {
    node_infos: Vec<NodeInfo>,
}

impl From<AddNodeRequest> for AppData {
    fn from(request: AddNodeRequest) -> Self {
        let mut data = Vec::new();
        data.push(data.len() as u8);
        for node in request.node_infos {
            data.extend(node.src_addr);
            data.push(node.protocol_type);
        }
        AppData::new(Afn::RouteSet, RouteSet::AddNode as u8, Some(data))
    }
}

pub struct DelNodeRequest {
    node_addrs: Vec<Address>,
}

impl From<DelNodeRequest> for AppData {
    fn from(request: DelNodeRequest) -> Self {
        let mut data = Vec::new();
        data.push(data.len() as u8);
        for addr in request.node_addrs {
            data.extend(addr);
        }
        AppData::new(Afn::RouteSet, RouteSet::DelNode as u8, Some(data))
    }
}
