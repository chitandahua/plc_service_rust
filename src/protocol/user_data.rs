use std::fmt::Formatter;
use std::fmt::{self, Display};

use anyhow::ensure;
use serde_json::map::Iter;

use crate::protocol::info_field::{self, InfoFieldType};
use crate::protocol::Address;
use crate::protocol::{AppData, InfoField};
use crate::Result;

// address field

pub fn hex_to_dec(bcd: u8) -> u8 {
    (bcd >> 4) * 10 + (bcd & 0x0f)
}

#[derive(Debug, Clone)]
pub struct AddressField {
    pub(crate) src_address: Address,
    pub(crate) relay_address: Option<Vec<Address>>,
    pub(crate) dst_address: Address,
}

impl AddressField {
    fn length(&self) -> usize {
        self.src_address.len()
            + self.relay_address.as_ref().map_or(0, |r| r.len())
            + self.dst_address.len()
    }
}

impl From<AddressField> for Vec<u8> {
    fn from(address_field: AddressField) -> Self {
        let mut data = vec![];
        data.extend(address_field.src_address);
        if let Some(relay_address) = address_field.relay_address {
            for address in relay_address {
                data.extend(address);
            }
        }
        data.extend(address_field.dst_address);
        data
    }
}

impl TryFrom<&[u8]> for AddressField {
    type Error = crate::Error;
    fn try_from(bytes: &[u8]) -> Result<Self> {
        let addr_len: usize = Address::default().len(); // TODO
        Ok(AddressField {
            // TODO
            src_address: bytes[..addr_len]
                .iter()
                .rev()
                .cloned()
                .collect::<Vec<_>>()
                .try_into()
                .unwrap(),
            relay_address: if bytes.len() > addr_len * 2 {
                Some(
                    bytes[addr_len..bytes.len() - addr_len]
                        .chunks(addr_len)
                        .map(|b| {
                            b.iter()
                                .rev()
                                .cloned()
                                .collect::<Vec<_>>()
                                .try_into()
                                .unwrap()
                        }) // b.try_into().unwrap(
                        .collect(),
                )
            } else {
                None
            },
            //dst_address: bytes[bytes.len() - addr_len..].try_into()?,
            dst_address: bytes[bytes.len() - addr_len..]
                .iter()
                .rev()
                .cloned()
                .collect::<Vec<_>>()
                .try_into()
                .unwrap(),
        })
    }
}

impl IntoIterator for AddressField {
    type Item = u8;
    type IntoIter = std::vec::IntoIter<Self::Item>;
    fn into_iter(self) -> Self::IntoIter {
        Into::<Vec<u8>>::into(self).into_iter()
    }
}

impl fmt::Display for AddressField {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "src_address: {}", hex::encode(self.src_address))?;
        if let Some(relay_address) = self.relay_address.as_ref() {
            for address in relay_address {
                writeln!(f, "relay_address: {}", hex::encode(address))?;
            }
        }
        writeln!(f, "dst_address: {}", hex::encode(self.dst_address))
    }
}

// user data
#[derive(Debug, Clone)]
pub struct UserData {
    pub(crate) info_field: InfoField,
    pub(crate) address_field: Option<AddressField>,
    pub(crate) app_data: AppData,
}

impl UserData {
    pub fn new(seq: u8, app_data: AppData) -> UserData {
        Self {
            info_field: InfoField::new(InfoFieldType::Down, seq, app_data.get_comm_mark()),
            address_field: None,
            app_data,
        }
    }

    pub fn into_app_data(self) -> AppData {
        self.app_data
    }

    pub fn length(&self) -> usize {
        info_field::INFO_FIELD_SIZE
            + self.address_field.as_ref().map(|a| a.length()).unwrap_or(0)
            + self.app_data.length()
    }

    pub fn from_bytes(info_field_type: InfoFieldType, bytes: &[u8]) -> Result<Self> {
        let info_field =
            InfoField::from_bytes(info_field_type, &bytes[0..info_field::INFO_FIELD_SIZE])?;
        let mut address_field = None;
        let app_data;
        if info_field.comm_model_mark() == 1 {
            // 中继地址数量为relay_level
            let relay_level = info_field.relay_level();
            let addr_len: usize = Address::default().len();
            let len = (relay_level as usize + 2) * addr_len;
            ensure!(
                bytes.len() >= info_field::INFO_FIELD_SIZE + len,
                "address field length error"
            );
            address_field = Some(
                bytes[info_field::INFO_FIELD_SIZE..info_field::INFO_FIELD_SIZE + len].try_into()?,
            );
            app_data = AppData::try_from(&bytes[info_field::INFO_FIELD_SIZE + len..])?;
        } else {
            app_data = AppData::try_from(&bytes[info_field::INFO_FIELD_SIZE..])?;
        }

        Ok(Self {
            info_field,
            address_field,
            app_data,
        })
    }

    pub fn get_seq(&self) -> u8 {
        self.info_field.seq_num()
    }
}

impl From<UserData> for Vec<u8> {
    fn from(user_data: UserData) -> Self {
        let mut data = vec![];
        data.extend(user_data.info_field);
        if let Some(address_field) = user_data.address_field {
            data.extend(address_field);
        }
        data.extend(user_data.app_data);
        data
    }
}

impl IntoIterator for UserData {
    type Item = u8;
    type IntoIter = std::vec::IntoIter<Self::Item>;
    fn into_iter(self) -> Self::IntoIter {
        //self.into().into_iter()
        Into::<Vec<u8>>::into(self).into_iter()
    }
}

impl Display for UserData {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "info_field: ")?;
        writeln!(f, "{}", self.info_field)?;
        if let Some(address_field) = self.address_field.as_ref() {
            writeln!(f, "address_field: ")?;
            writeln!(f, "{}", address_field)?;
        }
        writeln!(f, "app_data: ")?;
        writeln!(f, "{}", self.app_data)
    }
}
