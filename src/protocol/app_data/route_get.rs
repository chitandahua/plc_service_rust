use anyhow::ensure;
use chrono::NaiveDate;
use num_enum::TryFromPrimitive;

use crate::protocol::app_data::{
    date_transfer, slice_to_bcd_string, Address, Afn, AppDataError, ModuleIdFormat,
};
use crate::protocol::AppData;
use crate::Result;

use super::ADDR_LEN;

// AFN 10H
#[derive(Debug, TryFromPrimitive)]
#[repr(u8)]
pub enum RouteQuery {
    NodeNumber = 1,
    NodeInfo = 2,
    RunningStatus = 4,
    SlaveModuleId = 7,
    NetworkSize = 9,
    NetTopology = 21,
    NodeLineInfo = 31,
    IdInfo = 40,
    NetworkNodeInfo = 104,
    MultipleNet = 111,
    ChipInfo = 112,
}

pub struct NetworkNodeInfoRequest {
    start_index: u16,
    node_number: u8,
}

impl NetworkNodeInfoRequest {
    pub fn new(start_index: u16, node_number: u8) -> Self {
        Self {
            start_index,
            node_number,
        }
    }
}

impl From<NetworkNodeInfoRequest> for AppData {
    fn from(req: NetworkNodeInfoRequest) -> Self {
        let mut data = Vec::new();
        data.extend(req.start_index.to_le_bytes());
        data.push(req.node_number);
        AppData::new(Afn::RouteGet, RouteQuery::NetworkNodeInfo as u8, Some(data))
    }
}

pub struct NodeVersionInfo {
    pub address: Address,
    pub version: String,
    pub version_date: NaiveDate,
    pub factory_code: String,
    pub chip_code: String,
}

impl From<&[u8]> for NodeVersionInfo {
    fn from(data: &[u8]) -> Self {
        Self {
            address: data[0..6].try_into().unwrap(),
            version: slice_to_bcd_string(&data[6..8]),
            version_date: date_transfer(data[10], data[9], data[8]),
            factory_code: String::from_utf8(vec![data[12], data[11]]).unwrap(),
            chip_code: String::from_utf8(vec![data[14], data[13]]).unwrap(),
        }
    }
}

pub struct NetworkNodeInfoResponse {
    pub total_node_number: u16,
    _node_number: u8,
    pub node_version_infos: Vec<NodeVersionInfo>,
}

impl TryFrom<AppData> for NetworkNodeInfoResponse {
    type Error = crate::Error;

    fn try_from(app_data: AppData) -> Result<Self> {
        const PREFIX_SIZE: usize = 3;
        const NODE_VERSION_INFO_SIZE: usize = 15;
        ensure!(
            app_data.data_length() >= PREFIX_SIZE,
            AppDataError::DataLength(app_data.data_length())
        );

        let node_number = app_data.data_units.as_ref().unwrap()[2] as usize;
        app_data.check(
            Afn::RouteGet,
            RouteQuery::NetworkNodeInfo as u8,
            node_number * NODE_VERSION_INFO_SIZE + PREFIX_SIZE,
        )?;

        let data_units = app_data.data_units.unwrap();

        let total_node_number = u16::from_le_bytes(data_units[0..2].try_into()?);
        let node_version_infos = data_units[PREFIX_SIZE..]
            .chunks(NODE_VERSION_INFO_SIZE)
            .map(NodeVersionInfo::from)
            .collect();
        Ok(Self {
            total_node_number,
            _node_number: node_number as u8,
            node_version_infos,
        })
    }
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

#[derive(Debug, PartialEq)]
pub struct IdInfoRequest {
    pub device_type: u8,
    pub address: Address,
    pub id_type: u8,
}

impl From<IdInfoRequest> for AppData {
    fn from(req: IdInfoRequest) -> Self {
        let mut data = Vec::new();
        data.push(req.device_type);
        data.extend(req.address);
        data.push(req.id_type);
        AppData::new(Afn::RouteGet, RouteQuery::IdInfo as u8, Some(data))
    }
}

#[derive(Debug, PartialEq)]
pub struct IdInfoResponse {
    pub device_type: u8,
    pub address: Address,
    pub id_type: u8,
    pub id_length: u8,
    pub id_info: Vec<u8>,
}

const ID_INFO_SIZE: usize = 9;
impl TryFrom<AppData> for IdInfoResponse {
    type Error = crate::Error;

    fn try_from(app_data: AppData) -> Result<Self> {
        ensure!(
            app_data.data_length() >= ID_INFO_SIZE,
            AppDataError::DataLength(app_data.data_length())
        );

        let id_length = app_data.data_units.as_ref().unwrap()[8] as usize;
        app_data.check(
            Afn::RouteGet,
            RouteQuery::IdInfo as u8,
            id_length + ID_INFO_SIZE,
        )?;

        let data_units = app_data.data_units.unwrap();

        Ok(Self {
            device_type: data_units[0],
            address: data_units[1..7].try_into()?,
            id_type: data_units[7],
            id_length: id_length as u8,
            id_info: data_units[9..9 + id_length].to_vec(),
        })
    }
}

#[derive(Debug, PartialEq)]
pub struct QueryNodeLineInfoRequest {
    start_seq: u16,
    node_number: u8,
}

impl QueryNodeLineInfoRequest {
    pub fn new(start_seq: u16, node_number: u8) -> Self {
        Self {
            start_seq,
            node_number,
        }
    }
}

impl From<QueryNodeLineInfoRequest> for AppData {
    fn from(req: QueryNodeLineInfoRequest) -> Self {
        let mut data = Vec::new();
        data.extend(req.start_seq.to_le_bytes());
        data.push(req.node_number);
        AppData::new(Afn::RouteGet, RouteQuery::NodeLineInfo as u8, Some(data))
    }
}

const NODE_LINE_INFO_SIZE: usize = 8;
#[derive(Debug, PartialEq)]
pub struct NodeLineInfo {
    pub addr: Address,
    pub info: u16,
}

impl From<&[u8]> for NodeLineInfo {
    fn from(data: &[u8]) -> Self {
        Self {
            addr: data[0..6].try_into().unwrap(),
            info: u16::from_le_bytes(data[6..8].try_into().unwrap()),
        }
    }
}

const NODE_LINE_PREFIX_SIZE: usize = 5;
#[derive(Debug, PartialEq)]
pub struct QueryNodeLineInfoResponse {
    pub total_node_number: u16,
    pub start_index: u16,
    pub node_number: u8,
    pub line_infos: Vec<NodeLineInfo>,
}

impl TryFrom<AppData> for QueryNodeLineInfoResponse {
    type Error = crate::Error;

    fn try_from(app_data: AppData) -> Result<Self> {
        ensure!(
            app_data.data_length() >= NODE_LINE_PREFIX_SIZE,
            AppDataError::DataLength(app_data.data_length())
        );

        let node_number = app_data.data_units.as_ref().unwrap()[4] as usize;
        app_data.check(
            Afn::RouteGet,
            RouteQuery::NodeLineInfo as u8,
            node_number * NODE_LINE_INFO_SIZE + NODE_LINE_PREFIX_SIZE,
        )?;

        let data_units = app_data.data_units.unwrap();

        let total_node_number = u16::from_le_bytes(data_units[0..2].try_into()?);
        let start_index = u16::from_le_bytes(data_units[2..4].try_into()?);
        let line_infos = data_units[NODE_LINE_PREFIX_SIZE..]
            .chunks(NODE_LINE_INFO_SIZE)
            .take(node_number)
            .map(NodeLineInfo::from)
            .collect();
        Ok(Self {
            total_node_number,
            start_index,
            node_number: node_number as u8,
            line_infos,
        })
    }
}

#[derive(Debug, PartialEq)]
pub struct SlaveModuleIdRequest {
    pub start_seq: u16,
    pub node_number: u8,
}

impl SlaveModuleIdRequest {
    pub fn new(start_seq: u16, node_number: u8) -> Self {
        Self {
            start_seq,
            node_number,
        }
    }
}

impl From<SlaveModuleIdRequest> for AppData {
    fn from(req: SlaveModuleIdRequest) -> Self {
        let mut data = Vec::new();
        data.extend(req.start_seq.to_le_bytes());
        data.push(req.node_number);
        AppData::new(Afn::RouteGet, RouteQuery::SlaveModuleId as u8, Some(data))
    }
}

const SLAVE_MODULE_ID_PREFIX_SIZE: usize = 11;
#[derive(Debug, PartialEq)]
pub struct SlaveModuleIdInfo {
    pub address: Address,
    pub device_type: u8,
    pub factory_code: String,
    pub id_format: ModuleIdFormat,
    pub id_info: Vec<u8>,
}

impl TryFrom<&[u8]> for SlaveModuleIdInfo {
    type Error = crate::Error;
    fn try_from(data: &[u8]) -> Result<Self> {
        Ok(Self {
            address: data[0..ADDR_LEN].try_into().unwrap(),
            device_type: data[6],
            factory_code: String::from_utf8(vec![data[8], data[7]]).unwrap(),
            id_format: data[10].try_into()?,
            id_info: data[11..].to_vec(),
        })
    }
}

#[derive(Debug, PartialEq)]
pub struct SlaveModuleIdResponse {
    pub total_node_number: u16,
    pub node_number: u8,
    pub slave_module_id_infos: Vec<SlaveModuleIdInfo>,
}

const SLAVE_MODULE_ID_SIZE: usize = 3;
impl TryFrom<AppData> for SlaveModuleIdResponse {
    type Error = crate::Error;

    fn try_from(app_data: AppData) -> Result<Self> {
        ensure!(
            app_data.data_length() >= SLAVE_MODULE_ID_SIZE,
            AppDataError::DataLength(app_data.data_length())
        );

        let data_units = app_data.data_units.as_ref().unwrap();
        let node_number = data_units[2] as usize;
        let mut module_id_index = SLAVE_MODULE_ID_SIZE;
        let mut slave_module_id_infos = Vec::with_capacity(node_number);
        for _ in 0..node_number {
            ensure!(
                app_data.data_length() >= module_id_index + SLAVE_MODULE_ID_PREFIX_SIZE,
                AppDataError::DataLength(app_data.data_length())
            );
            let id_length = data_units[module_id_index + 9] as usize;
            let id_length = id_length + SLAVE_MODULE_ID_PREFIX_SIZE;
            ensure!(
                app_data.data_length() >= module_id_index + id_length,
                AppDataError::DataLength(app_data.data_length())
            );
            slave_module_id_infos.push(SlaveModuleIdInfo::try_from(
                &data_units[module_id_index..module_id_index + id_length],
            )?);
            module_id_index += id_length;
        }
        app_data.check(
            Afn::RouteGet,
            RouteQuery::SlaveModuleId as u8,
            module_id_index,
        )?;

        let total_node_number = u16::from_le_bytes(data_units[0..2].try_into()?);

        Ok(Self {
            total_node_number,
            node_number: node_number as u8,
            slave_module_id_infos,
        })
    }
}

#[derive(Debug, PartialEq)]
pub struct NetTopologyRequest {
    pub start_seq: u16,
    pub node_number: u8,
}

impl NetTopologyRequest {
    pub fn new(start_seq: u16, node_number: u8) -> Self {
        Self {
            start_seq,
            node_number,
        }
    }
}

impl From<NetTopologyRequest> for AppData {
    fn from(req: NetTopologyRequest) -> Self {
        let mut data = Vec::new();
        data.extend(req.start_seq.to_le_bytes());
        data.push(req.node_number);
        AppData::new(Afn::RouteGet, RouteQuery::NetTopology as u8, Some(data))
    }
}

const NODE_NETTOPOLOGY_INFO_SIZE: usize = 11;
#[derive(Debug, PartialEq)]
pub struct NetTopologyInfo {
    pub address: Address,
    pub node_flag: u16,
    pub proxy_node_flag: u16,
    pub node_level: u8,
    pub node_role: u8,
}

impl TryFrom<&[u8]> for NetTopologyInfo {
    type Error = crate::Error;
    fn try_from(data: &[u8]) -> Result<Self> {
        Ok(Self {
            address: data[0..ADDR_LEN].try_into().unwrap(),
            node_flag: u16::from_le_bytes(data[6..8].try_into()?),
            proxy_node_flag: u16::from_le_bytes(data[8..10].try_into()?),
            node_level: data[10] & 0x0F,
            node_role: data[10] >> 4,
        })
    }
}

#[derive(Debug, PartialEq)]
pub struct NetTopologyResponse {
    pub total_node_number: u16,
    pub start_index: u16,
    pub node_number: u8,
    pub net_topology_infos: Vec<NetTopologyInfo>,
}

const NET_TOPOLOGY_PREFIX_SIZE: usize = 5;
impl TryFrom<AppData> for NetTopologyResponse {
    type Error = crate::Error;

    fn try_from(app_data: AppData) -> Result<Self> {
        ensure!(
            app_data.data_length() >= NET_TOPOLOGY_PREFIX_SIZE,
            AppDataError::DataLength(app_data.data_length())
        );

        let data_units = app_data.data_units.as_ref().unwrap();
        let node_number = data_units[4] as usize;

        app_data.check(
            Afn::RouteGet,
            RouteQuery::NetTopology as u8,
            node_number * NODE_NETTOPOLOGY_INFO_SIZE + NET_TOPOLOGY_PREFIX_SIZE,
        )?;

        let net_topology_infos = data_units[NET_TOPOLOGY_PREFIX_SIZE..]
            .chunks(NODE_NETTOPOLOGY_INFO_SIZE)
            .take(node_number)
            .map(NetTopologyInfo::try_from)
            .collect::<Result<Vec<NetTopologyInfo>>>()?;
        Ok(Self {
            total_node_number: u16::from_le_bytes(data_units[0..2].try_into()?),
            start_index: u16::from_le_bytes(data_units[2..4].try_into()?),
            node_number: node_number as u8,
            net_topology_infos,
        })
    }
}

pub struct MultipleNetRequest;

impl From<MultipleNetRequest> for AppData {
    fn from(_: MultipleNetRequest) -> Self {
        AppData::new(Afn::RouteGet, RouteQuery::MultipleNet as u8, None)
    }
}

const MULTIPLE_NET_INFO_SIZE: usize = 3;
#[derive(Debug, PartialEq)]
pub struct MultipleNetInfo {
    pub net_identity: u32,
}

#[derive(Debug, PartialEq)]
pub struct MultipleNetResponse {
    pub total_node_number: u8,
    pub node_net_identity: u32,
    pub address: Address,
    pub multiple_net_infos: Vec<MultipleNetInfo>,
}

const MULTIPLE_NET_PREFIX_SIZE: usize = 10;
impl TryFrom<AppData> for MultipleNetResponse {
    type Error = crate::Error;

    fn try_from(app_data: AppData) -> Result<Self> {
        ensure!(
            app_data.data_length() >= MULTIPLE_NET_PREFIX_SIZE,
            AppDataError::DataLength(app_data.data_length())
        );

        let data_units = app_data.data_units.as_ref().unwrap();
        let total_node_number = data_units[0] as usize;

        app_data.check(
            Afn::RouteGet,
            RouteQuery::MultipleNet as u8,
            total_node_number * MULTIPLE_NET_INFO_SIZE + MULTIPLE_NET_PREFIX_SIZE,
        )?;

        let multiple_net_infos = data_units[MULTIPLE_NET_PREFIX_SIZE..]
            .chunks(MULTIPLE_NET_INFO_SIZE)
            .take(total_node_number)
            .map(|data| MultipleNetInfo {
                net_identity: u32::from_le_bytes([data[0], data[1], data[2], 0]),
            })
            .collect();
        Ok(Self {
            total_node_number: total_node_number as u8,
            node_net_identity: u32::from_le_bytes([data_units[1], data_units[2], data_units[3], 0]),
            address: data_units[4..10].try_into().unwrap(),
            multiple_net_infos,
        })
    }
}

pub struct RunningStatusRequest;

impl From<RunningStatusRequest> for AppData {
    fn from(_: RunningStatusRequest) -> Self {
        AppData::new(Afn::RouteGet, RouteQuery::RunningStatus as u8, None)
    }
}

#[allow(dead_code)]
#[derive(Debug, PartialEq)]
pub struct RunningStatus {
    error_code: u8,
    report_event_flag: u8,
    pub work_flag: u8,
    route_finish_flag: u8,
}

impl From<u8> for RunningStatus {
    fn from(value: u8) -> Self {
        Self {
            error_code: (value & 0xf0) >> 4,
            report_event_flag: (value & 0x04) >> 2,
            work_flag: (value & 0x02) >> 1,
            route_finish_flag: value & 0x01,
        }
    }
}

#[allow(dead_code)]
#[derive(Debug, PartialEq)]
pub struct WorkStatus {
    meter_status: u8,
    pub area_identify_status: u8,
    report_event_flag: u8,
    register_permit: u8,
    pub work_status: u8,
}

impl From<u8> for WorkStatus {
    fn from(value: u8) -> Self {
        Self {
            meter_status: (value & 0xc0) >> 6,
            area_identify_status: (value & 0x08) >> 3,
            report_event_flag: (value & 0x04) >> 2,
            register_permit: (value & 0x02) >> 1,
            work_status: value & 0x01,
        }
    }
}

#[allow(dead_code)]
#[derive(Debug, PartialEq)]
pub struct RunningStatusResponse {
    pub running_status: RunningStatus,
    total_node_number: u16,
    meter_node_number: u16,
    relay_meter_node_number: u16,
    pub work_status: WorkStatus,
    comm_speed: u16,
    relay_level: [u8; 3],
    work_step: [u8; 3],
}

#[derive(PartialEq, TryFromPrimitive)]
#[repr(u8)]
pub enum CurrentStatus {
    Metering = 0,
    Searching = 1,
    Upgrading = 2,
    Other = 3,
}

impl RunningStatusResponse {
    pub fn current_status(&self) -> CurrentStatus {
        CurrentStatus::try_from(self.work_status.meter_status).unwrap()
    }
}

impl TryFrom<AppData> for RunningStatusResponse {
    type Error = crate::Error;

    fn try_from(app_data: AppData) -> Result<Self> {
        app_data.check(Afn::RouteGet, RouteQuery::RunningStatus as u8, 16)?;

        let data_units = app_data.data_units.unwrap();

        Ok(Self {
            running_status: data_units[0].into(),
            total_node_number: u16::from_le_bytes([data_units[1], data_units[2]]),
            meter_node_number: u16::from_le_bytes([data_units[3], data_units[4]]),
            relay_meter_node_number: u16::from_le_bytes([data_units[5], data_units[6]]),
            work_status: data_units[7].into(),
            comm_speed: u16::from_le_bytes([data_units[8], data_units[9]]),
            relay_level: data_units[10..13].try_into().unwrap(),
            work_step: data_units[13..16].try_into().unwrap(),
        })
    }
}

pub struct NetworkSizeRequest;

impl From<NetworkSizeRequest> for AppData {
    fn from(_: NetworkSizeRequest) -> Self {
        AppData::new(Afn::RouteGet, RouteQuery::NetworkSize as u8, None)
    }
}

pub struct NetworkSizeResponse {
    pub network_size: u16,
}

impl TryFrom<AppData> for NetworkSizeResponse {
    type Error = crate::Error;

    fn try_from(app_data: AppData) -> Result<Self> {
        app_data.check(Afn::RouteGet, RouteQuery::NetworkSize as u8, 2)?;
        let data_units = app_data.data_units.unwrap();
        Ok(Self {
            network_size: u16::from_le_bytes([data_units[0], data_units[1]]),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::app_data::*;

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

    #[test]
    fn test_id_info_request() {
        let frame =
            tests_common::create_frame_from_hex("6817004300000000000010800401218967563412018616");
        let request = IdInfoRequest {
            device_type: 0x01,
            address: "123456678921".into(),
            id_type: 0x01,
        };
        assert_eq!(frame.into_app_data(), request.into());
    }

    #[test]
    fn test_id_info_response() {
        let frame = tests_common::create_frame_from_hex("68300083000010000005108004034841020100000118000000000000000000000000000000000000000000000000d416");
        let response = IdInfoResponse {
            device_type: 0x03,
            address: "000001024148".into(),
            id_type: 0x01,
            id_length: 0x18,
            id_info: hex::decode("000000000000000000000000000000000000000000000000").unwrap(),
        };

        assert_eq!(
            TryInto::<IdInfoResponse>::try_into(frame.into_app_data()).unwrap(),
            response
        );
    }

    #[test]
    fn test_chip_info_request() {
        let frame = tests_common::create_frame_from_hex("6812004300000000000010800d0100ffe016");
        let request = ChipInfoRequest {
            start_seq: 0x0001,
            node_number: 0xff,
        };
        assert_eq!(frame.into_app_data(), request.into());
    }

    #[test]
    fn test_chip_info_response() {
        let frame = tests_common::create_frame_from_hex(
            "6877008300001000000710800d030001000310445713576602000000000000000000000000000000000000000000000000082416433420200003ffffffffffffffffffffffffffffffffffffffffffffffff06090250000222220338722ac62e5b47f528bd9400003241484c03fbc1019c02010322ac16",
        );
        let response = ChipInfoResponse {
            total_node_number: 0x0003,
            start_seq: 0x0001,
            node_number: 0x03,
            chip_infos: vec![
                ChipInformation {
                    address: "665713574410".into(),
                    device_type: 0x02,
                    id_info: hex::decode("000000000000000000000000000000000000000000000000")
                        .unwrap()
                        .try_into()
                        .unwrap(),
                    software_version: "2408".into(),
                },
                ChipInformation {
                    address: "002020344316".into(),
                    device_type: 0x03,
                    id_info: hex::decode("ffffffffffffffffffffffffffffffffffffffffffffffff")
                        .unwrap()
                        .try_into()
                        .unwrap(),
                    software_version: "0906".into(),
                },
                ChipInformation {
                    address: "222202005002".into(),
                    device_type: 0x03,
                    id_info: hex::decode("38722ac62e5b47f528bd9400003241484c03fbc1019c0201")
                        .unwrap()
                        .try_into()
                        .unwrap(),
                    software_version: "2203".into(),
                },
            ],
        };
        assert_eq!(
            TryInto::<ChipInfoResponse>::try_into(frame.into_app_data()).unwrap(),
            response
        );
    }

    #[test]
    fn test_node_line_info_request() {
        let frame = tests_common::create_frame_from_hex("681200430000000000001040030100039a16");
        let request = QueryNodeLineInfoRequest {
            start_seq: 0x0001,
            node_number: 0x03,
        };
        assert_eq!(frame.into_app_data(), request.into());
    }

    #[test]
    fn test_node_line_info_response() {
        let frame = tests_common::create_frame_from_hex(
            "682400830000100000051040030300010002484102010000010012710201004511005a16",
        );
        let response = QueryNodeLineInfoResponse {
            total_node_number: 0x0003,
            start_index: 0x0001,
            node_number: 0x02,
            line_infos: vec![
                NodeLineInfo {
                    addr: "000001024148".into(),
                    info: 0x0001,
                },
                NodeLineInfo {
                    addr: "450001027112".into(),
                    info: 0x0011,
                },
            ],
        };

        assert_eq!(
            TryInto::<QueryNodeLineInfoResponse>::try_into(frame.into_app_data()).unwrap(),
            response
        );
    }

    #[test]
    fn test_slave_module_id_request() {
        let frame = tests_common::create_frame_from_hex("681200430000000000001040000100039716");
        let request = SlaveModuleIdRequest {
            start_seq: 0x0001,
            node_number: 0x03,
        };
        assert_eq!(frame.into_app_data(), request.into());
    }

    #[test]
    fn test_slave_module_id_response() {
        let frame = tests_common::create_frame_from_hex(
            "68340083000010000005104000030002000248410201814c4d060212710201004514524841020900484906023471520102483d16"
        );
        let response = SlaveModuleIdResponse {
            total_node_number: 0x0003,
            node_number: 0x02,
            slave_module_id_infos: vec![
                SlaveModuleIdInfo {
                    address: "010241480200".into(),
                    device_type: 0x81,
                    factory_code: "ML".into(),
                    id_format: ModuleIdFormat::Bin,
                    id_info: hex::decode("127102010045").unwrap(),
                },
                SlaveModuleIdInfo {
                    address: "090241485214".into(),
                    device_type: 0x00,
                    factory_code: "IH".into(),
                    id_format: ModuleIdFormat::Bin,
                    id_info: hex::decode("347152010248").unwrap(),
                },
            ],
        };

        assert_eq!(
            TryInto::<SlaveModuleIdResponse>::try_into(frame.into_app_data()).unwrap(),
            response
        );
    }

    #[test]
    fn test_net_topology_request() {
        let frame = tests_common::create_frame_from_hex("6812004300000000000310100201000a7316");
        let request = NetTopologyRequest {
            start_seq: 0x0001,
            node_number: 0x0a,
        };
        assert_eq!(frame.into_app_data(), request.into());
    }

    #[test]
    fn test_net_topology_response() {
        let frame = tests_common::create_frame_from_hex(
            "682a008300001000000510100203000100024841020100000100127112010045110001020001004a8716",
        );
        let response = NetTopologyResponse {
            total_node_number: 0x0003,
            start_index: 0x0001,
            node_number: 0x02,
            net_topology_infos: vec![
                NetTopologyInfo {
                    address: "000001024148".into(),
                    node_flag: 1,
                    proxy_node_flag: 28946,
                    node_level: 2,
                    node_role: 1,
                },
                NetTopologyInfo {
                    address: "010011450001".into(),
                    node_flag: 2,
                    proxy_node_flag: 1,
                    node_level: 10,
                    node_role: 4,
                },
            ],
        };

        assert_eq!(
            TryInto::<NetTopologyResponse>::try_into(frame.into_app_data()).unwrap(),
            response
        );
    }

    #[test]
    fn test_running_status_request() {
        let frame = tests_common::create_frame_from_hex("680f00430000000000031008005e16");

        assert_eq!(frame.into_app_data(), RunningStatusRequest {}.into());
    }

    #[test]
    fn test_running_status_response() {
        let frame = tests_common::create_frame_from_hex(
            "681f00830000100000031008000101025000022222f014431600000000a516",
        );
        let response = RunningStatusResponse {
            running_status: RunningStatus {
                error_code: 0,
                report_event_flag: 0,
                work_flag: 0,
                route_finish_flag: 1,
            },
            total_node_number: 0x0201,
            meter_node_number: 0x0050,
            relay_meter_node_number: 0x2202,
            work_status: WorkStatus {
                meter_status: 0,
                area_identify_status: 0,
                report_event_flag: 0,
                register_permit: 1,
                work_status: 0,
            },
            comm_speed: 0x14f0,
            relay_level: [0x43, 0x16, 0x00],
            work_step: [0x00, 0x00, 0x00],
        };

        assert_eq!(
            TryInto::<RunningStatusResponse>::try_into(frame.into_app_data()).unwrap(),
            response
        );
    }
}
