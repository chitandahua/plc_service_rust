use crate::protocol::app_data::Afn;
use crate::protocol::AppData;

// AFN 01H
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::app_data::*;
    use crate::protocol::Frame;

    #[test]
    fn test_init_request() {
        let frame_str = "680f00430000000000000102004616";
        let frame = tests_common::create_frame_from_hex(frame_str);

        let init_request = InitRequest::new(InitOperation::Params);
        assert_eq!(frame.into_app_data(), init_request.into());
    }
}
