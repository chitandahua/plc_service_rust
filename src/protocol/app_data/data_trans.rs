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
