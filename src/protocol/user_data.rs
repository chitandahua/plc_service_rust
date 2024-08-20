use std::fmt::Formatter;
use std::fmt::{self, Display};

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
struct AddressField {
    src_address: Address,
    relay_address: Option<Vec<Address>>,
    dst_address: Address,
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

impl IntoIterator for AddressField {
    type Item = u8;
    type IntoIter = std::vec::IntoIter<Self::Item>;
    fn into_iter(self) -> Self::IntoIter {
        Into::<Vec<u8>>::into(self).into_iter()
    }
}

// user data
#[derive(Debug, Clone)]
pub struct UserData {
    info_field: InfoField,
    address_field: Option<AddressField>,
    pub(crate) app_data: AppData,
}

impl UserData {
    pub fn new(seq: u8, app_data: AppData) -> UserData {
        Self {
            info_field: InfoField::new(InfoFieldType::Down, seq),
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
}

impl TryFrom<&[u8]> for UserData {
    type Error = anyhow::Error;
    fn try_from(data: &[u8]) -> Result<Self> {
        todo!()
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
        //writeln!(f, "address_field: ")?;
        //writeln!(f, "{}", self.address_field)?;
        writeln!(f, "app_data: ")?;
        writeln!(f, "{}", self.app_data)
    }
}
