use anyhow::ensure;
use chrono::NaiveDate;
use std::fmt;

use crate::protocol::app_data::{Address, Afn, AppDataError};
use crate::protocol::user_data::hex_to_dec;
use crate::protocol::AppData;
use crate::Result;

// AFN 03H
#[derive(Debug)]
#[repr(u8)]
pub enum QueryData {
    GetModuleInfo = 10,
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
    pub version: u16,
    pub comm_speed: Vec<u16>,
}

impl ModuleInfoResponse {
    fn date_transfer(year: u8, month: u8, day: u8) -> NaiveDate {
        //debug!("{:02x}-{:02x}-{:02x}", year, month, day);
        NaiveDate::from_ymd_opt(
            2000 + hex_to_dec(year) as i32,
            hex_to_dec(month) as u32,
            hex_to_dec(day) as u32,
        )
        .unwrap() // TODO
    }

    pub fn date_to_string(date: &NaiveDate) -> String {
        date.format("%Y%m%d").to_string()
    }
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

        let mut response = ModuleInfoResponse {
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
            protocol_release_date: Self::date_transfer(data_unit[26], data_unit[25], data_unit[24]),
            last_record_date: Self::date_transfer(data_unit[29], data_unit[28], data_unit[27]),
            factory_code: String::from_utf8(vec![data_unit[31], data_unit[30]]).unwrap(),
            chip_code: String::from_utf8(vec![data_unit[33], data_unit[32]]).unwrap(),
            version_date: Self::date_transfer(data_unit[36], data_unit[35], data_unit[34]),
            version: u16::from_le_bytes([data_unit[37], data_unit[38]]),
            comm_speed: Vec::new(),
            // Initialize other fields here...
            metering_mode: 0,
            node_info_mode: 0,
            route_management_mode: 0,
            broadcast_cmd_mode: 0,
            broadcast_cmd_confirm: 0,
            fail_node_change_mode: 0,
            delay_param_support: 0,
            low_voltage_power_off: 0,
            channel_num: 0,
        };

        for i in (0..speed_num as usize * 2).step_by(2) {
            response
                .comm_speed
                .push(u16::from_le_bytes([data_unit[39 + i], data_unit[40 + i]]));
        }

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
            Self::date_to_string(&self.protocol_release_date)
        )?;
        writeln!(
            f,
            "last_record_date: {}",
            Self::date_to_string(&self.last_record_date)
        )?;
        writeln!(f, "factory_code: {}", self.factory_code)?;
        writeln!(f, "chip_code: {}", self.chip_code)?;
        writeln!(
            f,
            "version_date: {}",
            Self::date_to_string(&self.version_date)
        )?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::app_data::*;
    use crate::protocol::Frame;

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
            NaiveDate::from_ymd_opt(2007, 8, 18)
        );
        assert_eq!(
            module_info_response.last_record_date,
            NaiveDate::from_ymd_opt(2033, 7, 30)
        );
        assert_eq!(module_info_response.factory_code, "xh");
        assert_eq!(module_info_response.chip_code, "bz");
        assert_eq!(
            module_info_response.version_date,
            NaiveDate::from_ymd_opt(2024, 12, 1)
        );

        assert_eq!(module_info_response.version, 0x2300);
        assert_eq!(module_info_response.comm_speed, vec![0x8221, 0x2300]);
    }

    #[test]
    fn test_date_transfer() {
        let date = NaiveDate::from_ymd_opt(2007, 8, 18);
        assert_eq!(ModuleInfoResponse::date_transfer(0x07, 0x08, 0x18), date);
    }

    #[test]
    fn test_date_to_string() {
        let date = NaiveDate::from_ymd_opt(2007, 8, 18);
        assert_eq!(ModuleInfoResponse::date_to_string(&date), "20070818");
    }
}
