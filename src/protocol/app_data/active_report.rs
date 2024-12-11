use num_enum::TryFromPrimitive;

use anyhow::ensure;

use crate::protocol::app_data::{Address, Afn, AppDataError};
use crate::protocol::AppData;
use crate::Result;

// AFN 06H
#[derive(Debug, TryFromPrimitive)]
#[repr(u8)]
pub enum ActiveReport {
    // 从节点信息
    NodeInfo = 1,
    // 工况变动
    WorkStatus = 3,
    // 从节点信息及设备类型
    NodeInfoAndDeviceType = 4,
    SlaveNodeEvent = 5,
}

const NODE_INFO_SIZE: usize = 9;
#[allow(dead_code)]
pub struct ReportNodeInfoDetail {
    pub address: Address,
    pub protocol_type: u8,
    pub seq_num: u16,
}

impl From<&[u8]> for ReportNodeInfoDetail {
    fn from(data: &[u8]) -> Self {
        Self {
            address: data[0..6].try_into().unwrap(),
            protocol_type: data[6],
            seq_num: u16::from_le_bytes(data[7..9].try_into().unwrap()),
        }
    }
}

#[allow(dead_code)]
pub struct ReportNodeInfo {
    pub node_number: u8,
    pub node_info_details: Vec<ReportNodeInfoDetail>,
}

impl TryFrom<AppData> for ReportNodeInfo {
    type Error = crate::Error;
    fn try_from(app_data: AppData) -> Result<Self> {
        ensure!(
            app_data.data_length() >= 1,
            AppDataError::DataLength(app_data.data_length())
        );

        let node_number = app_data.data_units.as_ref().unwrap()[0];
        app_data.check(
            Afn::Report,
            ActiveReport::NodeInfo as u8,
            1 + node_number as usize * NODE_INFO_SIZE,
        )?;

        let data_units = app_data.data_units.unwrap();
        let node_info_details = data_units[1..]
            .chunks(NODE_INFO_SIZE)
            .take(node_number as usize)
            .map(ReportNodeInfoDetail::from)
            .collect();

        Ok(ReportNodeInfo {
            node_number,
            node_info_details,
        })
    }
}

// 工作任务变动类型
#[derive(Debug, TryFromPrimitive, PartialEq)]
#[repr(u8)]
pub enum WorkStatusType {
    Meter = 1,
    Search = 2,
    IdentifyArea = 3,
    Other,
}

pub struct ReportWorkStatus {
    pub work_status_type: WorkStatusType,
}

impl TryFrom<AppData> for ReportWorkStatus {
    type Error = crate::Error;
    fn try_from(app_data: AppData) -> Result<Self> {
        app_data.check(Afn::Report, ActiveReport::WorkStatus as u8, 1)?;

        let data_units = app_data.data_units.unwrap();
        Ok(ReportWorkStatus {
            work_status_type: WorkStatusType::try_from(data_units[0])?,
        })
    }
}

const SLAVE_NODE_INFO_SIZE: usize = 7;
#[allow(dead_code)]
pub struct SlaveNodeCommInfo {
    pub address: Address,
    pub protocol_type: u8,
}

impl From<&[u8]> for SlaveNodeCommInfo {
    fn from(data: &[u8]) -> Self {
        Self {
            address: data[0..6].try_into().unwrap(),
            protocol_type: data[6],
        }
    }
}

#[allow(dead_code)]
pub struct ReportNodeInfoAndDeviceType {
    pub node_number: u8,
    pub node_info: ReportNodeInfoDetail,
    pub device_type: u8,
    // 下接从节点数量
    pub slave_node_count: u8,
    // 本次传输的从节点数量
    pub slave_node_number: u8,
    pub slave_node_comm_infos: Vec<SlaveNodeCommInfo>,
}

impl TryFrom<AppData> for ReportNodeInfoAndDeviceType {
    type Error = crate::Error;
    fn try_from(app_data: AppData) -> Result<Self> {
        ensure!(
            app_data.data_length() >= 13,
            AppDataError::DataLength(app_data.data_length())
        );

        let slave_node_number = app_data.data_units.as_ref().unwrap()[12];
        app_data.check(
            Afn::Report,
            ActiveReport::NodeInfoAndDeviceType as u8,
            13 + slave_node_number as usize * SLAVE_NODE_INFO_SIZE,
        )?;

        let data_units = app_data.data_units.unwrap();
        let slave_node_comm_infos = data_units[13..]
            .chunks(SLAVE_NODE_INFO_SIZE)
            .take(slave_node_number as usize)
            .map(SlaveNodeCommInfo::from)
            .collect();

        Ok(ReportNodeInfoAndDeviceType {
            node_number: data_units[0],
            node_info: ReportNodeInfoDetail::from(&data_units[1..10]),
            device_type: data_units[10],
            slave_node_count: data_units[11],
            slave_node_number,
            slave_node_comm_infos,
        })
    }
}

#[derive(Debug, PartialEq)]
pub struct SlaveNodeEvent {
    pub device_type: u8,
    pub protocol_type: u8,
    pub data_length: u8,
    pub data: Vec<u8>,
}

impl TryFrom<AppData> for SlaveNodeEvent {
    type Error = crate::Error;
    fn try_from(app_data: AppData) -> Result<Self> {
        ensure!(
            app_data.data_length() >= 3,
            AppDataError::DataLength(app_data.data_length())
        );

        let data_length = app_data.data_units.as_ref().unwrap()[2];
        app_data.check(
            Afn::Report,
            ActiveReport::SlaveNodeEvent as u8,
            3 + data_length as usize,
        )?;

        let data_units = app_data.data_units.unwrap();
        Ok(Self {
            device_type: data_units[0],
            protocol_type: data_units[1],
            data_length,
            data: data_units[3..3 + data_length as usize].to_vec(),
        })
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::protocol::app_data::*;

    #[test]
    fn test_slave_node_event() {
        let frame_str = "682200c3000010000001061000020110689228950000136811043434393820165316";
        let frame = tests_common::create_frame_from_hex(frame_str);

        let slave_node_event = SlaveNodeEvent {
            device_type: 0x02,
            protocol_type: 0x01,
            data_length: 0x10,
            data: tests_common::hex_to_bytes("68922895000013681104343439382016"),
        };
        assert_eq!(
            SlaveNodeEvent::try_from(frame.into_app_data()).unwrap(),
            slave_node_event
        );
    }
}
