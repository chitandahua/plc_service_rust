use num_enum::TryFromPrimitive;

use crate::protocol::app_data::Afn;
use crate::protocol::AppData;

// AFN 12H
#[derive(Debug, TryFromPrimitive)]
#[repr(u8)]
pub enum MeterControl {
    Restart = 1,
    Pause = 2,
    Resume = 3,
}

#[derive(Default)]
pub struct PauseMetering;
#[derive(Default)]
pub struct ResumeMetering;
#[derive(Default)]
pub struct RestartMetering;

impl From<PauseMetering> for AppData {
    fn from(_: PauseMetering) -> Self {
        AppData::new(Afn::RouteCtrl, MeterControl::Pause as u8, None)
    }
}

impl From<ResumeMetering> for AppData {
    fn from(_: ResumeMetering) -> Self {
        AppData::new(Afn::RouteCtrl, MeterControl::Resume as u8, None)
    }
}

impl From<RestartMetering> for AppData {
    fn from(_: RestartMetering) -> Self {
        AppData::new(Afn::RouteCtrl, MeterControl::Restart as u8, None)
    }
}
