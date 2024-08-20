use std::fmt::Formatter;
use std::fmt::{self, Display};

use crate::protocol::app_data::Afn;
use crate::protocol::AppData;

#[derive(Debug)]
#[repr(u8)]
pub enum InitOperation {
    Hard = 1,
    Params = 2,
    Data = 3,
}

#[derive(Debug)]
pub struct InitRequest {
    operation: InitOperation,
}

impl InitRequest {
    pub fn new(operation: InitOperation) -> Self {
        Self { operation }
    }
}

impl From<InitRequest> for AppData {
    fn from(init_request: InitRequest) -> Self {
        AppData::new(Afn::Init, init_request.operation as u8, None)
    }
}
