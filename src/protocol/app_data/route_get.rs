use anyhow::ensure;
use num_enum::TryFromPrimitive;

use crate::protocol::app_data::{Address, Afn, AppDataError};
use crate::protocol::AppData;
use crate::Result;

use super::ADDR_LEN;

// AFN 10H
#[derive(Debug, TryFromPrimitive)]
#[repr(u8)]
pub enum RouteQuery {
    NodeNumber = 1,
    NodeInfo = 2,
    ChipInfo = 112,
}

#[derive(Debug)]
pub struct QueryNodeNumberRequest;

impl From<QueryNodeNumberRequest> for AppData {
    fn from(_: QueryNodeNumberRequest) -> Self {
        AppData::new(Afn::RouteGet, RouteQuery::NodeNumber as u8, None)
    }
}

#[derive(Debug, PartialEq)]
pub struct QueryNodeNumberResponse {
    pub node_number: u16,
    max_node_number: u16,
}

impl TryFrom<AppData> for QueryNodeNumberResponse {
    type Error = crate::Error;

    fn try_from(app_data: AppData) -> Result<Self> {
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

#[derive(Debug, PartialEq)]
pub struct QueryNodeInfoRequest {
    start_seq: u16,
    node_number: u8,
}

impl QueryNodeInfoRequest {
    pub fn new(start_seq: u16, node_number: u8) -> Self {
        Self {
            start_seq,
            node_number,
        }
    }
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
#[derive(Debug, PartialEq)]
pub struct NodeDetail {
    pub src_addr: Address,
    listen_signal_quality: u8,
    relay_level: u8,
    pub comm_protocol_type: u8,
    phase: u8,
}

impl From<&[u8]> for NodeDetail {
    fn from(data: &[u8]) -> Self {
        Self {
            src_addr: data[0..6].try_into().unwrap(),
            listen_signal_quality: data[6] >> 4,
            relay_level: data[6] & 0x0F,
            comm_protocol_type: (data[7] >> 3) & 0xfe,
            phase: data[7] & 0x07,
        }
    }
}

const NODE_NUMBER_SIZE: usize = 3;
#[derive(Debug, PartialEq)]
pub struct QueryNodeInfoResponse {
    total_node_number: u16,
    node_number: u8,
    node_infos: Vec<NodeDetail>,
}

impl QueryNodeInfoResponse {
    pub fn into_node_infos(self) -> Vec<NodeDetail> {
        self.node_infos
    }
}

impl TryFrom<AppData> for QueryNodeInfoResponse {
    type Error = crate::Error;

    fn try_from(app_data: AppData) -> Result<Self> {
        ensure!(
            app_data.data_length() >= NODE_NUMBER_SIZE,
            AppDataError::DataLength(app_data.data_length())
        );

        let node_number = app_data.data_units.as_ref().unwrap()[2] as usize;
        app_data.check(
            Afn::RouteGet,
            RouteQuery::NodeInfo as u8,
            node_number * NODE_INFO_SIZE + NODE_NUMBER_SIZE,
        )?;

        let data_units = app_data.data_units.unwrap();

        let total_node_number = u16::from_le_bytes(data_units[0..2].try_into()?);
        let node_infos = data_units[NODE_NUMBER_SIZE..]
            .chunks(NODE_INFO_SIZE)
            .take(node_number)
            .map(NodeDetail::from)
            .collect();
        Ok(Self {
            total_node_number,
            node_number: node_number as u8,
            node_infos,
        })
    }
}

#[derive(Debug, PartialEq)]
pub struct ChipInfoRequest {
    pub start_seq: u16,
    pub node_number: u8,
}

impl ChipInfoRequest {
    pub fn new(start_seq: u16, node_number: u8) -> Self {
        Self {
            start_seq,
            node_number,
        }
    }
}

impl From<ChipInfoRequest> for AppData {
    fn from(req: ChipInfoRequest) -> Self {
        let mut data = Vec::new();
        data.extend(req.start_seq.to_le_bytes());
        data.push(req.node_number);
        AppData::new(Afn::RouteGet, RouteQuery::ChipInfo as u8, Some(data))
    }
}

const CHIP_INFO_DATA_LEN: usize = 33;
#[derive(Debug, PartialEq)]
pub struct ChipInformation {
    pub address: Address,
    pub device_type: u8,
    pub id_info: [u8; 24],
    pub software_version: String,
}

impl From<&[u8]> for ChipInformation {
    fn from(data: &[u8]) -> Self {
        Self {
            address: data[0..ADDR_LEN].try_into().unwrap(),
            device_type: data[6],
            id_info: data[7..31].try_into().unwrap(),
            software_version: hex::encode([data[32], data[31]]),
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct ChipInfoResponse {
    pub total_node_number: u16,
    pub start_seq: u16,
    pub node_number: u8,
    pub chip_infos: Vec<ChipInformation>,
}

const CHIP_INFO_SIZE: usize = 5;
impl TryFrom<AppData> for ChipInfoResponse {
    type Error = crate::Error;

    fn try_from(app_data: AppData) -> Result<Self> {
        ensure!(
            app_data.data_length() >= CHIP_INFO_SIZE,
            AppDataError::DataLength(app_data.data_length())
        );

        let node_number = app_data.data_units.as_ref().unwrap()[4] as usize;
        app_data.check(
            Afn::RouteGet,
            RouteQuery::ChipInfo as u8,
            node_number * CHIP_INFO_DATA_LEN + CHIP_INFO_SIZE,
        )?;

        let data_units = app_data.data_units.unwrap();

        let total_node_number = u16::from_le_bytes(data_units[0..2].try_into()?);
        let start_seq = u16::from_le_bytes(data_units[2..4].try_into()?);

        let chip_infos = data_units[CHIP_INFO_SIZE..]
            .chunks(CHIP_INFO_DATA_LEN)
            .fold(Vec::new(), |mut acc, x| {
                acc.push(ChipInformation::from(x));
                acc
            });

        Ok(Self {
            total_node_number,
            start_seq,
            node_number: node_number as u8,
            chip_infos,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::app_data::*;
    use crate::Result;
    use tests_common::create_frame_from_hex;

    #[test]
    fn test_query_node_number_request() {
        let frame = tests_common::create_frame_from_hex("680f00430000000000001001005416");
        assert_eq!(frame.into_app_data(), QueryNodeNumberRequest {}.into());
    }

    #[test]
    fn test_query_node_number_response() {
        let frame = tests_common::create_frame_from_hex("681300830000100000001001000100f807a416");
        let response = QueryNodeNumberResponse {
            node_number: 0x0001,
            max_node_number: 0x07f8,
        };
        assert_eq!(
            TryInto::<QueryNodeNumberResponse>::try_into(frame.into_app_data()).unwrap(),
            response
        );
    }

    #[test]
    fn test_query_node_info_request() {
        let frame = tests_common::create_frame_from_hex("68120043000000802500100200000001fb16");
        let request = QueryNodeInfoRequest {
            start_seq: 0x0000,
            node_number: 0x01,
        };
        assert_eq!(frame.into_app_data(), request.into());
    }

    #[test]
    fn test_query_node_info_response() {
        let frame = tests_common::create_frame_from_hex(
            "681a0083000010000000100200010001025000022222f0144316",
        );
        let response = QueryNodeInfoResponse {
            total_node_number: 0x0001,
            node_number: 0x01,
            node_infos: vec![NodeDetail {
                src_addr: Address::new([0x22, 0x22, 0x02, 0x00, 0x50, 0x02]),
                listen_signal_quality: 0x0f,
                relay_level: 0x00,
                comm_protocol_type: 0x02,
                phase: 0x04,
            }],
        };
        assert_eq!(
            TryInto::<QueryNodeInfoResponse>::try_into(frame.into_app_data()).unwrap(),
            response
        );
    }
}
