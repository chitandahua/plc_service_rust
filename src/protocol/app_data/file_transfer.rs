use num_enum::TryFromPrimitive;

use crate::protocol::app_data::Afn;
use crate::protocol::AppData;
use crate::Result;

// AFN 15H
#[derive(Debug, TryFromPrimitive)]
#[repr(u8)]
pub enum FileTransfer {
    Method = 1,
}

#[derive(Debug, TryFromPrimitive, Clone, Copy)]
#[repr(u8)]
pub enum FileFlag {
    Clear = 0,
    Module = 3,
    MasterAndSlave = 7,
    Slave = 8,
}

#[derive(Debug)]
pub struct FileTransferRequest {
    file_flag: FileFlag,
    file_attr: u8,
    file_command: u8,
    segment_num: u16,
    segment_offset: u32,
    segment_len: u16,
    file_data: Vec<u8>,
}

impl FileTransferRequest {
    pub fn new(
        file_flag: FileFlag,
        segment_num: u16,
        segment_offset: u32,
        file_data: Vec<u8>,
    ) -> Self {
        Self {
            file_flag,
            file_attr: if segment_offset == segment_num as u32 - 1 {
                1
            } else {
                0
            },
            file_command: 0,
            segment_num,
            segment_offset,
            segment_len: file_data.len() as u16,
            file_data,
        }
    }
}

impl From<FileTransferRequest> for AppData {
    fn from(value: FileTransferRequest) -> Self {
        let mut data = Vec::new();
        data.push(value.file_flag as u8);
        data.push(value.file_attr);
        data.extend(value.file_command.to_le_bytes());
        data.extend(value.segment_num.to_le_bytes());
        data.extend(value.segment_offset.to_le_bytes());
        data.extend(value.segment_len.to_le_bytes());
        data.extend(value.file_data);

        AppData::new(Afn::FileTransfer, FileTransfer::Method as u8, Some(data))
    }
}

pub const _FILE_CHECK_ERROR: u32 = 0xffff;
pub struct FileTransferResponse {
    pub segment_flag: u32,
}

impl TryFrom<AppData> for FileTransferResponse {
    type Error = crate::Error;
    fn try_from(app_data: AppData) -> Result<Self> {
        app_data.check(Afn::FileTransfer, FileTransfer::Method as u8, 4)?;
        Ok(Self {
            segment_flag: u32::from_le_bytes(
                app_data.data_units.unwrap()[0..4].try_into().unwrap(),
            ),
        })
    }
}
