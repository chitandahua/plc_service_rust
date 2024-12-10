use anyhow::ensure;
use chrono::NaiveDate;
use num_enum::TryFromPrimitive;
use std::fmt;

use crate::protocol::app_data::{
    date_to_string, date_transfer, slice_to_bcd_string, Address, Afn, AppDataError, ModuleIdFormat,
};
use crate::protocol::AppData;
use crate::Result;

use super::ADDR_LEN;

// AFN 03H
#[derive(Debug, PartialEq, TryFromPrimitive)]
#[repr(u8)]
pub enum QueryData {
    CommModuleInfo = 1,
    MasterAddress = 4,
    BroadcastDelay = 9,
    GetModuleInfo = 10,
    GetMasterIdInfo = 12,
    HplcFrequency = 16,
}

pub struct CommModuleInfoRequest;

impl From<CommModuleInfoRequest> for AppData {
    fn from(_: CommModuleInfoRequest) -> Self {
        AppData::new(Afn::QueryData, QueryData::CommModuleInfo as u8, None)
    }
}

pub struct CommModuleInfoResponse {
    pub factory_code: String,
    pub chip_code: String,
    pub version_date: NaiveDate,
    pub version: String,
}

impl TryFrom<AppData> for CommModuleInfoResponse {
    type Error = crate::Error;
    fn try_from(app_data: AppData) -> Result<Self> {
        app_data.check(Afn::QueryData, QueryData::CommModuleInfo as u8, 9)?;
        let data_unit = app_data.data_units.unwrap();
        Ok(CommModuleInfoResponse {
            factory_code: String::from_utf8(vec![data_unit[1], data_unit[0]]).unwrap(),
            chip_code: String::from_utf8(vec![data_unit[3], data_unit[2]]).unwrap(),
            version_date: date_transfer(data_unit[6], data_unit[5], data_unit[4]),
            version: slice_to_bcd_string(&data_unit[7..9]),
        })
    }
}

pub struct MasterAddressRequest;

impl From<MasterAddressRequest> for AppData {
    fn from(_: MasterAddressRequest) -> Self {
        AppData::new(Afn::QueryData, QueryData::MasterAddress as u8, None)
    }
}

pub struct MasterAddressResponse {
    pub master_addr: Address,
}

impl TryFrom<AppData> for MasterAddressResponse {
    type Error = crate::Error;
    fn try_from(app_data: AppData) -> Result<Self> {
        app_data.check(Afn::QueryData, QueryData::MasterAddress as u8, ADDR_LEN)?;
        let data_unit = app_data.data_units.unwrap();
        Ok(MasterAddressResponse {
            master_addr: data_unit[0..6].try_into().unwrap(),
        })
    }
}

pub struct BroadcastDelayRequest {
    protocol_type: u8,
    message: Vec<u8>,
}

impl BroadcastDelayRequest {
    pub fn new(protocol_type: u8, message: Vec<u8>) -> Self {
        Self {
            protocol_type,
            message,
        }
    }
}

impl From<BroadcastDelayRequest> for AppData {
    fn from(value: BroadcastDelayRequest) -> Self {
        let mut data = Vec::new();
        data.push(value.protocol_type);
        data.push(value.message.len() as u8);
        data.extend(value.message);
        AppData::new(Afn::QueryData, QueryData::BroadcastDelay as u8, Some(data))
    }
}

const PREFIX_LEN: usize = 4;
#[derive(Debug, PartialEq)]
#[allow(dead_code)]
pub struct BroadcastDelayResponse {
    pub delay: u16,
    protocol_type: u8,
    message_len: u8,
    message: Vec<u8>,
}

impl TryFrom<AppData> for BroadcastDelayResponse {
    type Error = crate::Error;
    fn try_from(app_data: AppData) -> Result<Self> {
        ensure!(
            app_data.data_length() >= PREFIX_LEN,
            AppDataError::DataLength(app_data.data_length())
        );
        let message_len = app_data.data_units.as_ref().unwrap()[3] as usize;
        app_data.check(
            Afn::QueryData,
            QueryData::BroadcastDelay as u8,
            PREFIX_LEN + message_len,
        )?;

        let data_units = app_data.data_units.unwrap();
        Ok(BroadcastDelayResponse {
            delay: u16::from_le_bytes(data_units[0..2].try_into().unwrap()),
            protocol_type: data_units[2],
            message_len: message_len as u8,
            message: data_units[PREFIX_LEN..].to_vec(),
        })
    }
}

pub struct ModuleInfoRequest;

impl From<ModuleInfoRequest> for AppData {
    fn from(_: ModuleInfoRequest) -> Self {
        AppData::new(Afn::QueryData, QueryData::GetModuleInfo as u8, None)
    }
}

#[allow(dead_code)]
pub struct ModuleInfoResponse {
    pub metering_mode: u8,
    pub node_info_mode: u8,
    pub route_management_mode: u8,
    pub comm_mode: u8,
    pub broadcast_cmd_mode: u8,
    pub broadcast_cmd_confirm: u8,
    pub fail_node_change_mode: u8,
    pub delay_param_support: u8,
    pub low_voltage_power_off: u8,
    pub channel_num: u8,
    pub speed_num: u8,
    pub max_timeout_time: u8,
    pub broadcast_cmd_timeout_time: u16,
    pub max_packet_length: u16,
    pub max_packet_per_packet: u16,
    pub upgrade_wait_time: u8,
    pub main_node_addr: Address,
    pub max_node_num: u16,
    pub current_node_num: u16,
    pub protocol_release_date: NaiveDate,
    pub last_record_date: NaiveDate,
    pub factory_code: String,
    pub chip_code: String,
    pub version_date: NaiveDate,
    pub version: String,
    pub comm_speed: Vec<u16>,
}

impl TryFrom<AppData> for ModuleInfoResponse {
    type Error = crate::Error;
    fn try_from(app_data: AppData) -> Result<Self> {
        ensure!(
            app_data.data_length() >= 39,
            AppDataError::DataLength(app_data.data_length())
        );

        let speed_num = app_data.data_units.as_ref().unwrap()[3] & 0x0F;
        app_data.check(
            Afn::QueryData,
            QueryData::GetModuleInfo as u8,
            39 + speed_num as usize * 2,
        )?;
        let data_unit = app_data.data_units.unwrap();

        let response = ModuleInfoResponse {
            comm_mode: (data_unit[0] >> 4) & 0x0F,
            speed_num,
            max_timeout_time: data_unit[6],
            broadcast_cmd_timeout_time: u16::from_le_bytes([data_unit[7], data_unit[8]]),
            max_packet_length: u16::from_le_bytes([data_unit[9], data_unit[10]]),
            max_packet_per_packet: u16::from_le_bytes([data_unit[11], data_unit[12]]),
            upgrade_wait_time: data_unit[13],
            main_node_addr: data_unit[14..20].try_into().unwrap(),
            max_node_num: u16::from_le_bytes([data_unit[20], data_unit[21]]),
            current_node_num: u16::from_le_bytes([data_unit[22], data_unit[23]]),
            protocol_release_date: date_transfer(data_unit[26], data_unit[25], data_unit[24]),
            last_record_date: date_transfer(data_unit[29], data_unit[28], data_unit[27]),
            factory_code: String::from_utf8(vec![data_unit[31], data_unit[30]]).unwrap(),
            chip_code: String::from_utf8(vec![data_unit[33], data_unit[32]]).unwrap(),
            version_date: date_transfer(data_unit[36], data_unit[35], data_unit[34]),
            version: slice_to_bcd_string(&data_unit[37..39]),
            comm_speed: data_unit[39..]
                .chunks(2)
                .map(|x| u16::from_le_bytes(x.try_into().unwrap()))
                .collect(),
            // Initialize other fields here...
            metering_mode: (data_unit[0] & 0xc0) >> 6,
            node_info_mode: 0,
            route_management_mode: 0,
            broadcast_cmd_mode: 0,
            broadcast_cmd_confirm: 0,
            fail_node_change_mode: 0,
            delay_param_support: 0,
            low_voltage_power_off: 0,
            channel_num: 0,
        };

        Ok(response)
    }
}

impl fmt::Display for ModuleInfoResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "comm_mode: {}", self.comm_mode)?;
        writeln!(f, "max_timeout_time: {}", self.max_timeout_time)?;
        writeln!(
            f,
            "broadcast_cmd_timeout_time: {}",
            self.broadcast_cmd_timeout_time
        )?;
        writeln!(f, "max_packet_length: {}", self.max_packet_length)?;
        writeln!(f, "max_packet_per_packet: {}", self.max_packet_per_packet)?;
        writeln!(f, "upgrade_wait_time: {}", self.upgrade_wait_time)?;
        writeln!(f, "main_node_addr: {}", self.main_node_addr)?;
        writeln!(f, "max_node_num: {}", self.max_node_num)?;
        writeln!(f, "current_node_num: {}", self.current_node_num)?;
        writeln!(
            f,
            "protocol_release_date: {}",
            date_to_string(&self.protocol_release_date)
        )?;
        writeln!(
            f,
            "last_record_date: {}",
            date_to_string(&self.last_record_date)
        )?;
        writeln!(f, "factory_code: {}", self.factory_code)?;
        writeln!(f, "chip_code: {}", self.chip_code)?;
        writeln!(f, "version_date: {}", date_to_string(&self.version_date))?;
        writeln!(f, "version: {}", self.version)?;
        writeln!(f, "comm_speed:")?;
        for speed in &self.comm_speed {
            writeln!(
                f,
                "\tspeed: {}, unit_flag: {}",
                speed & 0x7fff,
                (speed >> 15) & 0x01
            )?;
        }
        Ok(())
    }
}

pub struct MasterIdInfoRequest;

impl From<MasterIdInfoRequest> for AppData {
    fn from(_: MasterIdInfoRequest) -> Self {
        AppData::new(Afn::QueryData, QueryData::GetMasterIdInfo as u8, None)
    }
}

pub struct MasterIdInfoResponse {
    pub factory_code: String,
    pub module_id_length: u8,
    pub module_id_format: ModuleIdFormat,
    pub module_id: Vec<u8>,
}

impl TryFrom<AppData> for MasterIdInfoResponse {
    type Error = crate::Error;
    fn try_from(app_data: AppData) -> Result<Self> {
        ensure!(
            app_data.data_length() >= 3,
            AppDataError::DataLength(app_data.data_length())
        );
        app_data.check(
            Afn::QueryData,
            QueryData::GetMasterIdInfo as u8,
            4 + app_data.data_units.as_ref().unwrap()[2] as usize,
        )?;
        let data_unit = app_data.data_units.unwrap();

        Ok(Self {
            factory_code: String::from_utf8(vec![data_unit[1], data_unit[0]]).unwrap(),
            module_id_length: data_unit[2],
            module_id_format: data_unit[3].try_into()?,
            module_id: data_unit[4..].to_vec(),
        })
    }
}

pub struct GetHplcFreqRequest;

impl From<GetHplcFreqRequest> for AppData {
    fn from(_: GetHplcFreqRequest) -> Self {
        AppData::new(Afn::QueryData, QueryData::HplcFrequency as u8, None)
    }
}

pub struct GetHplcFreqResponse {
    pub frequency: u8,
}

impl TryFrom<AppData> for GetHplcFreqResponse {
    type Error = crate::Error;
    fn try_from(app_data: AppData) -> Result<Self> {
        app_data.check(Afn::QueryData, QueryData::HplcFrequency as u8, 1)?;
        let data_unit = app_data.data_units.unwrap();
        Ok(Self {
            frequency: data_unit[0],
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::app_data::*;

    #[test]
    fn test_module_info_request() {
        let frame_str = "680f00430000000000000302014916";
        let frame = tests_common::create_frame_from_hex(frame_str);

        let module_info_request = ModuleInfoRequest {};
        assert_eq!(frame.into_app_data(), module_info_request.into());
    }

    #[test]
    fn test_module_info_response() {
        let frame_str = "68270083000000000004030201000000020000030500ffff12341078901268010243001c0018080730073368787a620112240023218200233c16";
        let frame = tests_common::create_frame_from_hex(frame_str);

        let module_info_response = ModuleInfoResponse::try_from(frame.into_app_data()).unwrap();
        assert_eq!(module_info_response.comm_mode, 0);
        assert_eq!(module_info_response.max_timeout_time, 0x03);
        assert_eq!(module_info_response.broadcast_cmd_timeout_time, 0x0005);
        assert_eq!(module_info_response.max_packet_length, 0xffff);
        assert_eq!(module_info_response.max_packet_per_packet, 0x3412);
        assert_eq!(module_info_response.upgrade_wait_time, 0x10);
        assert_eq!(
            module_info_response.main_node_addr,
            Address::new([0x02, 0x01, 0x68, 0x12, 0x90, 0x78])
        );
        assert_eq!(module_info_response.max_node_num, 0x0043);
        assert_eq!(module_info_response.current_node_num, 0x001c);
        assert_eq!(
            module_info_response.protocol_release_date,
            NaiveDate::from_ymd_opt(2007, 8, 18).unwrap()
        );
        assert_eq!(
            module_info_response.last_record_date,
            NaiveDate::from_ymd_opt(2033, 7, 30).unwrap()
        );
        assert_eq!(module_info_response.factory_code, "xh");
        assert_eq!(module_info_response.chip_code, "bz");
        assert_eq!(
            module_info_response.version_date,
            NaiveDate::from_ymd_opt(2024, 12, 1).unwrap()
        );

        assert_eq!(module_info_response.version, "2300");
        assert_eq!(module_info_response.comm_speed, vec![0x8221, 0x2300]);
    }

    #[test]
    fn test_date_transfer() {
        let date = NaiveDate::from_ymd_opt(2007, 8, 18).unwrap();
        assert_eq!(date_transfer(0x07, 0x08, 0x18), date);
    }

    #[test]
    fn test_date_to_string() {
        let date = NaiveDate::from_ymd_opt(2007, 8, 18).unwrap();
        assert_eq!(date_to_string(&date), "20070818");
    }

    #[test]
    fn test_master_id_info_request() {
        let frame = tests_common::create_frame_from_hex("680f00430000000000000308014f16");

        let master_id_info_request = MasterIdInfoRequest {};
        assert_eq!(frame.into_app_data(), master_id_info_request.into());
    }

    #[test]
    fn test_master_id_info_response() {
        let frame = tests_common::create_frame_from_hex(
            "681e0083000010000005030801484c0b0100000000000000000000004416",
        );

        let master_id_info_response =
            MasterIdInfoResponse::try_from(frame.into_app_data()).unwrap();
        assert_eq!(master_id_info_response.factory_code, "LH");
        assert_eq!(master_id_info_response.module_id_length, 0x0b);
        assert_eq!(
            master_id_info_response.module_id_format,
            ModuleIdFormat::Bcd
        );
        assert_eq!(
            master_id_info_response.module_id,
            hex::decode("0000000000000000000000").unwrap()
        );
    }

    #[test]
    fn test_broadcast_delay_request() {
        let frame_str = "682300430000286400960301010212689999999999996808064a33343a3c54ef167216";
        let frame = tests_common::create_frame_from_hex(frame_str);

        let broadcast_delay_request = BroadcastDelayRequest {
            protocol_type: 0x02,
            message: tests_common::hex_to_bytes("689999999999996808064a33343a3c54ef16"),
        };

        assert_eq!(frame.into_app_data(), broadcast_delay_request.into());
    }

    #[test]
    fn test_broadcast_delay_response() {
        let frame_str =
            "6825008300000000009603010101000212689999999999996808064a33343a3c54ef162716";
        let frame = tests_common::create_frame_from_hex(frame_str);
        let broadcast_delay_response = BroadcastDelayResponse {
            delay: 1,
            protocol_type: 0x02,
            message_len: 0x12,
            message: tests_common::hex_to_bytes("689999999999996808064a33343a3c54ef16"),
        };

        let response: BroadcastDelayResponse = frame.into_app_data().try_into().unwrap();
        assert_eq!(response, broadcast_delay_response);
    }
}
