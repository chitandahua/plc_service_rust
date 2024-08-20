use anyhow::ensure;
use chrono::NaiveDate;
use std::fmt::Formatter;
use std::fmt::{self, Display};

use crate::protocol::app_data::{Address, Afn, AppDataError};
use crate::protocol::user_data::hex_to_dec;
use crate::protocol::AppData;
use crate::Result;

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
        NaiveDate::from_ymd(
            2000 + hex_to_dec(year) as i32,
            hex_to_dec(month) as u32,
            hex_to_dec(day) as u32,
        )
    }

    pub fn date_to_string(date: &NaiveDate) -> String {
        date.format("%Y%m%d").to_string()
    }

    pub fn from_app_data(app_data: AppData) -> Result<Self> {
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

        let main_node_addr = data_unit[14..20].iter().rev().cloned().collect::<Vec<_>>();
        let mut response = ModuleInfoResponse {
            comm_mode: (data_unit[0] >> 4) & 0x0F,
            speed_num,
            max_timeout_time: data_unit[6],
            broadcast_cmd_timeout_time: u16::from_le_bytes([data_unit[7], data_unit[8]]),
            max_packet_length: u16::from_le_bytes([data_unit[9], data_unit[10]]),
            max_packet_per_packet: u16::from_le_bytes([data_unit[11], data_unit[12]]),
            upgrade_wait_time: data_unit[13],
            main_node_addr: main_node_addr.try_into().unwrap(),
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
        writeln!(f, "main_node_addr: {}", hex::encode(self.main_node_addr))?;
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
