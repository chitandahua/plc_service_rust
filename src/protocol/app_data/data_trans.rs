use anyhow::ensure;
use num_enum::TryFromPrimitive;

use crate::protocol::app_data::{Afn, AppDataError};
use crate::protocol::AppData;
use crate::Result;

// AFN 02H
#[derive(Debug, TryFromPrimitive)]
#[repr(u8)]
pub enum DataTransfer {
    TransferFrame = 1,
}

#[derive(Debug)]
pub struct TransferFrameRequest {
    protocol_type: u8,
    message: Vec<u8>,
}

impl TransferFrameRequest {
    pub fn new(protocol_type: u8, message: Vec<u8>) -> Self {
        Self {
            protocol_type,
            message,
        }
    }
}

impl From<TransferFrameRequest> for AppData {
    fn from(req: TransferFrameRequest) -> Self {
        let mut data = Vec::new();
        data.push(req.protocol_type);
        data.push(req.message.len() as u8);
        data.extend(req.message);
        AppData::new(
            Afn::DataForward,
            DataTransfer::TransferFrame as u8,
            Some(data),
        )
    }
}

const PREFIX_LEN: usize = 2;
#[derive(Debug, PartialEq)]
pub struct TransferFrameResponse {
    pub protocol_type: u8,
    pub message_len: u8,
    pub message: Vec<u8>,
}

impl TryFrom<AppData> for TransferFrameResponse {
    type Error = crate::Error;

    fn try_from(app_data: AppData) -> Result<Self> {
        ensure!(
            app_data.data_length() >= PREFIX_LEN,
            AppDataError::DataLength(app_data.data_length())
        );
        let message_len = app_data.data_units.as_ref().unwrap()[1] as usize;
        app_data.check(
            Afn::DataForward,
            DataTransfer::TransferFrame as u8,
            PREFIX_LEN + message_len,
        )?;

        let data_units = app_data.data_units.unwrap();
        Ok(TransferFrameResponse {
            protocol_type: data_units[0],
            message_len: message_len as u8,
            message: data_units[PREFIX_LEN..].to_vec(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::app_data::*;

    #[test]
    fn test_data_transfer_request() {
        let frame_str = "682d004304000000000677206419826301000000000002010001106801000000000068110435343433b616dd16";
        let frame = tests_common::create_frame_from_hex(frame_str);

        let data_transfer_request = TransferFrameRequest {
            protocol_type: 0x01,
            message: tests_common::hex_to_bytes("6801000000000068110435343433b616"),
        };

        assert_eq!(frame.into_app_data(), data_transfer_request.into());
    }

    #[test]
    fn test_data_transfer_response() {
        let frame_str = "681f0083000000000006020100020e6812345678901268010243c3ac16ed16";
        let frame = tests_common::create_frame_from_hex(frame_str);
        let data_transfer_response = TransferFrameResponse {
            protocol_type: 0x02,
            message_len: 0x0e,
            message: tests_common::hex_to_bytes("6812345678901268010243c3ac16"),
        };

        let response: TransferFrameResponse = frame.into_app_data().try_into().unwrap();
        assert_eq!(response, data_transfer_response);
    }
}
