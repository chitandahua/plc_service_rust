use num_enum::TryFromPrimitive;

use crate::protocol::app_data::{Address, Afn};
use crate::protocol::AppData;

// AFN 05H
#[derive(Debug, TryFromPrimitive)]
#[repr(u8)]
pub enum CtrlCmd {
    SetAddress = 1,
}

pub struct AddressSetRequest {
    address: Address,
}

impl AddressSetRequest {
    pub fn new(address: Address) -> Self {
        Self { address }
    }
}

impl From<AddressSetRequest> for AppData {
    fn from(address_set_request: AddressSetRequest) -> Self {
        AppData::new(
            Afn::CtrlCmd,
            CtrlCmd::SetAddress as u8,
            Some(address_set_request.address.into()),
        )
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Add;

    use super::*;
    use crate::protocol::app_data::*;
    use crate::protocol::Frame;

    #[test]
    fn test_address_set_request() {
        let frame_str = "68150043000000000000050100ab89675634128016";
        let frame = tests_common::create_frame_from_hex(frame_str);

        let address_set = AddressSetRequest {
            address: Address::new([0x12, 0x34, 0x56, 0x67, 0x89, 0xab]),
        };
        assert_eq!(frame.into_app_data(), address_set.into());
    }
}
