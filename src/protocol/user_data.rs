use std::fmt::Formatter;
use std::fmt::{self, Display};

use anyhow::ensure;

use crate::protocol::info_field::{self, InfoFieldType};
use crate::protocol::{Address, AppData, InfoField, ADDR_LEN};
use crate::Result;

// address field

pub fn hex_to_dec(bcd: u8) -> u8 {
    (bcd >> 4) * 10 + (bcd & 0x0f)
}

pub fn dec_to_hex(value: u8) -> u8 {
    ((value / 10) % 10) * 16 + (value % 10)
}

#[derive(Debug, Clone)]
pub struct AddressField {
    pub(crate) src_address: Address,
    pub(crate) relay_address: Option<Vec<Address>>,
    pub(crate) dst_address: Address,
}

impl AddressField {
    pub fn new(
        src_address: Address,
        relay_address: Option<Vec<Address>>,
        dst_address: Address,
    ) -> Self {
        Self {
            src_address,
            relay_address,
            dst_address,
        }
    }

    fn length(&self) -> usize {
        ADDR_LEN * (2 + self.relay_address.as_ref().map_or(0, |r| r.len()))
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
        Ok(AddressField {
            src_address: bytes[..ADDR_LEN].try_into().unwrap(),
            relay_address: if bytes.len() > ADDR_LEN * 2 {
                Some(
                    bytes[ADDR_LEN..bytes.len() - ADDR_LEN]
                        .chunks(ADDR_LEN)
                        .map(|b| b.try_into().unwrap())
                        .collect(),
                )
            } else {
                None
            },
            dst_address: bytes[bytes.len() - ADDR_LEN..].try_into().unwrap(),
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
        writeln!(f, "src_address: {}", self.src_address)?;
        if let Some(relay_address) = self.relay_address.as_ref() {
            for address in relay_address {
                writeln!(f, "relay_address: {}", address)?;
            }
        }
        writeln!(f, "dst_address: {}", self.dst_address)
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
    pub fn new(seq: u8, address_field: Option<AddressField>, app_data: AppData) -> UserData {
        Self {
            info_field: InfoField::new(
                InfoFieldType::Down,
                seq,
                address_field
                    .as_ref()
                    .map_or(0, |a| a.relay_address.is_some() as u8),
                app_data.get_comm_mark(),
            ),
            address_field,
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
        let app_data = if info_field.comm_model_mark() == 1 {
            // 中继地址数量为relay_level
            let relay_level = info_field.relay_level();
            let len = (relay_level as usize + 2) * ADDR_LEN;
            ensure!(
                bytes.len() >= info_field::INFO_FIELD_SIZE + len,
                "address field length error"
            );
            address_field = Some(
                bytes[info_field::INFO_FIELD_SIZE..info_field::INFO_FIELD_SIZE + len].try_into()?,
            );
            AppData::try_from(&bytes[info_field::INFO_FIELD_SIZE + len..])?
        } else {
            AppData::try_from(&bytes[info_field::INFO_FIELD_SIZE..])?
        };

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::app_data::{Afn, AppData};

    // Helper function to create a sample AppData
    fn create_sample_app_data() -> AppData {
        AppData::new(Afn::Answer, 1, Some(vec![0x01, 0x02, 0x03]))
    }

    #[test]
    fn test_hex_to_dec() {
        assert_eq!(hex_to_dec(0x12), 12);
        assert_eq!(hex_to_dec(0x99), 99);
        assert_eq!(hex_to_dec(0x00), 0);
        assert_eq!(hex_to_dec(0xFF), 165);
    }

    mod address_field_tests {
        use super::*;

        #[test]
        fn test_address_field_creation() {
            let src_address = Address::new([0x01, 0x02, 0x03, 0x04, 0x05, 0x06]);
            let dst_address = Address::new([0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C]);
            let address_field = AddressField {
                src_address,
                relay_address: None,
                dst_address,
            };

            assert_eq!(address_field.length(), 12);
            assert_eq!(
                address_field.src_address,
                Address::new([0x01, 0x02, 0x03, 0x04, 0x05, 0x06])
            );
            assert_eq!(
                address_field.dst_address,
                Address::new([0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C])
            );
        }

        #[test]
        fn test_address_field_with_relay() {
            let src_address = Address::new([0x01, 0x02, 0x03, 0x04, 0x05, 0x06]);
            let relay_address = vec![Address::new([0x0D, 0x0E, 0x0F, 0x10, 0x11, 0x12])];
            let dst_address = Address::new([0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C]);
            let address_field = AddressField {
                src_address,
                relay_address: Some(relay_address.clone()),
                dst_address,
            };

            assert_eq!(address_field.length(), 18);
            assert_eq!(
                address_field.src_address,
                Address::new([0x01, 0x02, 0x03, 0x04, 0x05, 0x06])
            );
            assert_eq!(address_field.relay_address, Some(relay_address));
            assert_eq!(
                address_field.dst_address,
                Address::new([0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C])
            );
        }

        #[test]
        fn test_address_field_conversion() {
            let src_address = Address::new([0x01, 0x02, 0x03, 0x04, 0x05, 0x06]);
            let relay_address = vec![Address::new([0x0D, 0x0E, 0x0F, 0x10, 0x11, 0x12])];
            let dst_address = Address::new([0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C]);
            let address_field = AddressField {
                src_address,
                relay_address: Some(relay_address),
                dst_address,
            };

            let bytes: Vec<u8> = address_field.clone().into();
            assert_eq!(bytes.len(), 18);

            let reconstructed = AddressField::try_from(bytes.as_slice()).unwrap();
            assert_eq!(reconstructed.src_address, address_field.src_address);
            assert_eq!(reconstructed.relay_address, address_field.relay_address);
            assert_eq!(reconstructed.dst_address, address_field.dst_address);
        }

        #[test]
        fn test_address_field_display() {
            let src_address = Address::new([0x01, 0x02, 0x03, 0x04, 0x05, 0x06]);
            let relay_address = vec![Address::new([0x0D, 0x0E, 0x0F, 0x10, 0x11, 0x12])];
            let dst_address = Address::new([0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C]);
            let address_field = AddressField {
                src_address,
                relay_address: Some(relay_address),
                dst_address,
            };

            let display_string = format!("{}", address_field);
            assert!(display_string.contains("src_address: 010203040506"));
            assert!(display_string.contains("relay_address: 0d0e0f101112"));
            assert!(display_string.contains("dst_address: 0708090a0b0c"));
        }
    }

    mod user_data_tests {
        use super::*;

        #[test]
        fn test_user_data_creation() {
            let app_data = create_sample_app_data();
            let user_data = UserData::new(1, None, app_data.clone());

            assert_eq!(user_data.get_seq(), 1);
            assert_eq!(user_data.app_data, app_data);
            assert!(user_data.address_field.is_none());
        }

        #[test]
        fn test_user_data_length() {
            let app_data = create_sample_app_data();
            let mut user_data = UserData::new(1, None, app_data);

            let length_without_address = user_data.length();

            user_data.address_field = Some(AddressField {
                src_address: Address::new([0x01, 0x02, 0x03, 0x04, 0x05, 0x06]),
                relay_address: None,
                dst_address: Address::new([0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C]),
            });

            let length_with_address = user_data.length();
            assert_eq!(length_with_address - length_without_address, 12);
        }

        #[test]
        fn test_user_data_from_bytes() {
            let app_data = AppData::new(
                Afn::RouteDataForward,
                1,
                Some(vec![0x01, 0x02, 0x00, 0x01, 0x02, 0x03]),
            );
            let address_field = Some(AddressField {
                src_address: Address::new([0x01, 0x02, 0x03, 0x04, 0x05, 0x06]),
                relay_address: None,
                dst_address: Address::new([0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C]),
            });
            let user_data = UserData::new(1, address_field, app_data);

            let bytes: Vec<u8> = user_data.clone().into();
            let reconstructed = UserData::from_bytes(InfoFieldType::Down, &bytes).unwrap();

            assert_eq!(reconstructed.get_seq(), user_data.get_seq());
            assert_eq!(reconstructed.app_data, user_data.app_data);
            assert_eq!(
                reconstructed.address_field.is_some(),
                user_data.address_field.is_some()
            );
        }

        #[test]
        fn test_user_data_conversion() {
            let app_data = create_sample_app_data();
            let user_data = UserData::new(1, None, app_data.clone());

            let bytes: Vec<u8> = user_data.clone().into();
            assert!(!bytes.is_empty());

            let reconstructed = UserData::from_bytes(InfoFieldType::Down, &bytes).unwrap();
            assert_eq!(reconstructed.get_seq(), user_data.get_seq());
            assert_eq!(reconstructed.app_data, user_data.app_data);
        }

        #[test]
        fn test_user_data_display() {
            let app_data = create_sample_app_data();
            let user_data = UserData::new(1, None, app_data);

            let display_string = format!("{}", user_data);
            assert!(display_string.contains("info_field:"));
            assert!(display_string.contains("app_data:"));
        }

        #[test]
        fn test_user_data_into_app_data() {
            let app_data = create_sample_app_data();
            let user_data = UserData::new(1, None, app_data.clone());

            let extracted_app_data = user_data.into_app_data();
            assert_eq!(extracted_app_data, app_data);
        }
    }
}
