use std::fmt::Formatter;
use std::fmt::{self, Display};

use crate::protocol::app_data::{Address, Afn};
use crate::protocol::AppData;

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
    fn from(mut address_set_request: AddressSetRequest) -> Self {
        address_set_request.address.reverse();
        AppData::new(
            Afn::CtrlCmd,
            CtrlCmd::SetAddress as u8,
            Some(address_set_request.address.into()),
        )
    }
}
