use std::fmt::Formatter;
use std::fmt::{self, Display};
use anyhow::ensure;

use crate::protocol::AppData;
use crate::protocol::app_data::{Afn, Address};

pub enum RouteQuery {
    NodeNumber = 1,
    NodeInfo = 2,
}

pub struct QueryNodeNumberRequest;

impl From<QueryNodeNumberRequest> for AppData {
    fn from(_: QueryNodeNumberRequest) -> Self {
        AppData::new(Afn::RouteGet, RouteQuery::NodeNumber as u8, None)
    }
}

pub struct QueryNodeNumberResponse {
    node_number: u16,
    max_node_number: u16,
}

impl TryFrom<AppData> for QueryNodeNumberResponse {
    type Error = crate::Error;

    fn try_from(app_data: AppData) -> Result<Self, Self::Error> {
        app_data.check(Afn::RouteGet, RouteQuery::NodeNumber as u8, 4)?;

        let data_units = app_data.data_units.unwrap();
        let node_number = u16::from_le_bytes(data_units[0..2].try_into()?);
        let max_node_number = u16::from_le_bytes(data_units[2..4].try_into()?);

        Ok(Self {
            node_number,
            max_node_number,
        })
    }
}

pub struct QueryNodeInfoRequest {
    start_seq: u16,
    node_number: u8,
}

impl From<QueryNodeInfoRequest> for AppData {
    fn from(req: QueryNodeInfoRequest) -> Self {
        let mut data = Vec::new();
        data.extend(req.start_seq.to_le_bytes());
        data.push(req.node_number);
        AppData::new(Afn::RouteGet, RouteQuery::NodeInfo as u8, Some(data))
    }
}

const NODE_INFO_SIZE: usize = 8;
pub struct NodeInfo {
    src_addr: Address,
    listen_signal_quality: u8,
    relay_level: u8,
    comm_protocol_type: u8,
    phase: u8,
}

impl From<&[u8]> for NodeInfo {
    fn from(data: &[u8]) -> Self {
        Self {
            src_addr: data[0..6].try_into().unwrap(),
            listen_signal_quality: data[6] >> 4,
            relay_level: data[6] & 0x0F,
            comm_protocol_type: (data[7] >> 6) & 0x08,
            phase: data[7] & 0x08,
        }
    }
}

const NODE_NUMBER_SIZE: usize = 3;
pub struct QueryNodeInfoResponse {
    total_node_number: u16,
    node_number: u8,
    node_infos: Vec<NodeInfo>,
}

impl TryFrom<AppData> for QueryNodeInfoResponse {
    type Error = crate::Error;

    fn try_from(app_data: AppData) -> Result<Self, Self::Error> {
        ensure!(
            app_data.data_length() >= NODE_NUMBER_SIZE,
            AppDataError::DataLength
        );

        let node_number = app_data.data_units.unwrap()[2] as usize;
        app_data.check(
            Afn::RouteGet,
            RouteQuery::NodeInfo as u8,
            node_number * NODE_INFO_SIZE + NODE_NUMBER_SIZE,
        )?;

        let data_units = app_data.data_units.unwrap();

        let total_node_number = u16::from_le_bytes(data_units[0..2].try_into()?);
        let mut node_infos = Vec::new();
        for i in 0..node_number {
            node_infos.push(NodeInfo::from(
                &data_units[(i * NODE_INFO_SIZE + NODE_NUMBER_SIZE)..],
            ));
        }
        Ok(Self {
            total_node_number,
            node_number,
            node_infos,
        })
    }
}
