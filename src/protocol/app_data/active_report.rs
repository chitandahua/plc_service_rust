use num_enum::TryFromPrimitive;

use anyhow::ensure;

use crate::protocol::app_data::{Afn, AppDataError};
use crate::protocol::AppData;
use crate::Result;

// AFN 06H
#[derive(Debug, TryFromPrimitive)]
#[repr(u8)]
pub enum ActiveReport {
    SlaveNodeEvent = 5,
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
