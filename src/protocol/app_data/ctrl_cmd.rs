use num_enum::TryFromPrimitive;

use crate::protocol::app_data::{Address, Afn};
use crate::protocol::AppData;

// AFN 05H
#[derive(Debug, TryFromPrimitive)]
#[repr(u8)]
pub enum CtrlCmd {
    SetAddress = 1,
    Broadcast = 3,
    IdentifyArea = 6,
}

pub struct BroadcastRequest {
    protocol_type: u8,
    message: Vec<u8>,
}

impl BroadcastRequest {
    pub fn new(protocol_type: u8, message: Vec<u8>) -> Self {
        Self {
            protocol_type,
            message,
        }
    }
}

impl From<BroadcastRequest> for AppData {
    fn from(value: BroadcastRequest) -> Self {
        let mut data = Vec::new();
        data.push(value.protocol_type);
        data.push(value.message.len() as u8);
        data.extend(value.message);
        AppData::new(Afn::CtrlCmd, CtrlCmd::Broadcast as u8, Some(data))
    }
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

pub struct IdentifyAreaSetRequest {
    enable_flag: u8,
}

impl IdentifyAreaSetRequest {
    pub fn new(enable_flag: u8) -> Self {
        Self { enable_flag }
    }
}

impl From<IdentifyAreaSetRequest> for AppData {
    fn from(value: IdentifyAreaSetRequest) -> Self {
        AppData::new(
            Afn::CtrlCmd,
            CtrlCmd::IdentifyArea as u8,
            Some(vec![value.enable_flag]),
        )
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::protocol::app_data::*;

    #[test]
    fn test_address_set_request() {
        let frame_str = "68150043000000000000050100ab89675634128016";
        let frame = tests_common::create_frame_from_hex(frame_str);

        let address_set = AddressSetRequest {
            address: Address::new([0x12, 0x34, 0x56, 0x67, 0x89, 0xab]),
        };
        assert_eq!(frame.into_app_data(), address_set.into());
    }

    #[test]
    fn test_broadcast_request() {
        let frame_str = "682300430000286400990504000212689999999999996808064A33343A3C54EF167916";
        let frame = tests_common::create_frame_from_hex(frame_str);

        let broadcast = BroadcastRequest::new(
            0x02,
            hex::decode("689999999999996808064A33343A3C54EF16").unwrap(),
        );
        assert_eq!(frame.into_app_data(), broadcast.into());
    }
}
