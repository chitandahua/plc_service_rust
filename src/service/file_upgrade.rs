use std::fs::File;
use std::io::prelude::*;
use std::ops::Deref;
use std::sync::{mpsc, Arc, Condvar, Mutex};
use std::time::Duration;

use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};

use crate::mqtt_handler::MqttTopicType;
use crate::mqtt_message::{MqttMessage, PayloadBody};
use crate::protocol::app_data::{
    Afn, CurrentStatus, FileFlag, FileTransfer, FileTransferRequest, FileTransferResponse,
    RunningStatusRequest, RunningStatusResponse,
};
use crate::protocol::Frame;
use crate::request_info::{FrameKey, ReqInfo, UartMessage};
use crate::service::parse_response::{mqtt_info_request_uart_handler, UartResponse};
use crate::service::{IntoMqttMessage, MqttReqInfo, ThreadPool};
use crate::{
    impl_into_mqtt_message, register_mqtt_request_topics, MqttMsgHandler, MqttResponseError, Result,
};

#[derive(Debug, Clone, Copy, PartialEq, Default)]
#[repr(u8)]
enum UpgradeStatus {
    #[default]
    Idle = 0,
    Upgrading = 1,
    Finished = 2,
}

#[derive(Debug, Clone, Copy, Default)]
#[repr(u8)]
enum UpgradeResult {
    #[default]
    Success = 0,
    FileError = 1,
    TransferFailed = 2,
    _OtherError = 255,
}

#[derive(Debug, Default)]
struct FileUpgradeState {
    status: UpgradeStatus,
    start_time: NaiveDateTime,
    end_time: NaiveDateTime,
    upgrade_result: UpgradeResult,
    result: Option<Result<usize>>,
}

#[derive(Debug, Serialize)]
struct MqttFileUpgradeResponse {
    #[serde(rename = "upgradeStatus")]
    upgrade_status: u8,
    #[serde(rename = "starttime")]
    start_time: String,
    #[serde(rename = "endtime")]
    end_time: String,
    #[serde(rename = "result")]
    upgrade_result: u8,
}

impl_into_mqtt_message!(MqttFileUpgradeResponse, flat);

impl From<&FileUpgradeState> for MqttFileUpgradeResponse {
    fn from(state: &FileUpgradeState) -> Self {
        Self {
            upgrade_status: state.status as u8,
            start_time: state.start_time.format("%Y-%m-%d %H:%M:%S").to_string(),
            end_time: state.end_time.format("%Y-%m-%d %H:%M:%S").to_string(),
            upgrade_result: state.upgrade_result as u8,
        }
    }
}

impl FileUpgradeState {
    fn set_upgrade_result(&mut self, upgrate_result: UpgradeResult) {
        self.status = UpgradeStatus::Finished;
        self.end_time = chrono::Local::now().naive_local();
        self.upgrade_result = upgrate_result;
    }
}

#[derive(Clone)]
pub struct FileUpgrade {
    upgrade_state: Arc<Mutex<FileUpgradeState>>,
    cond: Arc<Condvar>,
}

#[derive(Debug, Deserialize)]
struct MqttFileUpgradeRequest {
    flag: u8,
    #[serde(rename = "filePath")]
    file_path: String,
}

impl FileUpgrade {
    pub fn new() -> Self {
        Self {
            upgrade_state: Arc::new(Mutex::new(FileUpgradeState::default())),
            cond: Arc::new(Condvar::new()),
        }
    }

    pub fn init(mqtt_msg_handler: &mut MqttMsgHandler) {
        register_mqtt_request_topics!(
            mqtt_msg_handler,
            (
                "action",
                "startFileUpgrade",
                MqttTopicType::FileUpgrade,
                "start_file_upgrade_schema.json"
            ),
            (
                "get",
                "fileUpgradeState",
                MqttTopicType::UpgradeState,
                "get_file_upgrade_state_schema.json"
            )
        );
    }

    pub fn mqtt_file_upgrade(
        &self,
        message: MqttMessage,
        mqtt_msg_sender: &mpsc::Sender<MqttMessage>,
        uart_msg_sender: &mpsc::Sender<UartMessage>,
        thread_pool: &ThreadPool,
    ) -> Result<()> {
        tracing::debug!("mqtt file upgrade");
        // 回复确认
        mqtt_msg_sender
            .send(().into_mqtt_message(message.to_mqtt_req_info()))
            .unwrap();

        const FILE_DATA_SIZE: usize = 128;
        let request: MqttFileUpgradeRequest = serde_json::from_str(message.payload())?;

        let file_flag = match request.flag {
            0 => FileFlag::Module,
            1 => FileFlag::MasterAndSlave,
            2 => FileFlag::Slave,
            _ => unreachable!(),
        };

        // 传输文件
        {
            let mut state = self.upgrade_state.lock().unwrap();
            state.status = UpgradeStatus::Idle;
            state.start_time = chrono::Local::now().naive_local(); // TODO 等待传输完成才开始？

            // 读文件到buffer中
            let mut file = match File::open(&request.file_path) {
                Ok(file) => file,
                Err(e) => {
                    tracing::error!(cause = ?e, "open file {} failed", request.file_path);
                    state.set_upgrade_result(UpgradeResult::FileError);
                    return Ok(());
                }
            };

            let mut content = Vec::new();
            file.read_to_end(&mut content).unwrap();
            let segment_num = content.len() / FILE_DATA_SIZE;

            for (segment_offset, file_data) in content.chunks(FILE_DATA_SIZE).enumerate() {
                mqtt_info_request_uart_handler::<FileTransferRequest>(
                    FileTransferRequest::new(
                        file_flag,
                        segment_num as u16,
                        segment_offset as u32,
                        file_data.to_vec(),
                    ),
                    Some(message.to_mqtt_req_info()),
                    uart_msg_sender,
                );

                state = self
                    .cond
                    .wait_while(state, |state| state.result.is_none())
                    .unwrap();

                let result = state.result.take().unwrap().and_then(|segment| {
                    if segment != segment_offset {
                        Err(anyhow::anyhow!(format!(
                            "segment number not match, expect {}, got {}",
                            segment_offset, segment
                        )))
                    } else {
                        Ok(())
                    }
                });

                if let Err(e) = result {
                    tracing::error!(cause = ?e, "uart file transfer failed");
                    state.set_upgrade_result(UpgradeResult::TransferFailed);
                    return Ok(());
                }
            }
        }

        thread_pool.execute({
            let file_upgrade = self.clone();
            let uart_msg_sender = uart_msg_sender.clone();
            move || {
                let mut state = file_upgrade.upgrade_state.lock().unwrap();
                // MODULE_INFO.get().unwrap().upgrade_wait_time as u64
                let duration = Duration::from_secs(30);
                state.status = UpgradeStatus::Upgrading;
                while state.status == UpgradeStatus::Upgrading {
                    let result = file_upgrade
                        .cond
                        .wait_timeout_while(state, duration, |state| state.result.is_none())
                        .unwrap();
                    state = result.0;
                    if result.1.timed_out() {
                        // 查询状态
                        let frame = Frame::new_request(None, RunningStatusRequest);
                        let req_info = ReqInfo::new(&frame, None);
                        uart_msg_sender
                            .send(UartMessage::new_with_extra_req_info(
                                req_info,
                                frame,
                                Some(ReqInfo::new_with_key_no_seq(
                                    FrameKey::new(Afn::FileTransfer, FileTransfer::Method as u8),
                                    None,
                                )),
                            ))
                            .unwrap();

                        // 等待回复
                        state = file_upgrade
                            .cond
                            .wait_while(state, |state| state.result.is_none())
                            .unwrap();
                        if let Err(e) = state.result.take().unwrap() {
                            tracing::error!(cause = ?e, "get running status failed");
                        }
                    } else {
                        state.result.take();
                    }
                }

                state.set_upgrade_result(UpgradeResult::Success);
            }
        });

        Ok(())
    }

    pub fn uart_file_transfer_timeout(&self) {
        let mut state = self.upgrade_state.lock().unwrap();
        state.result = Some(Err(anyhow::anyhow!(MqttResponseError::Timeout)));
        self.cond.notify_one();
    }

    pub fn uart_file_transfer_finish(&self, message: UartMessage) -> Result<()> {
        let response = UartResponse::<RunningStatusResponse>::try_from(message.frame)?;
        let result: Result<RunningStatusResponse> = response.into();

        let mut state = self.upgrade_state.lock().unwrap();
        match result {
            Ok(status) => {
                if status.current_status() != CurrentStatus::Upgrading {
                    tracing::debug!("upgrade finished");
                    state.status = UpgradeStatus::Finished;
                }
                state.result = Some(Ok(0));
            }
            Err(err) => state.result = Some(Err(err)),
        }
        self.cond.notify_one();

        Ok(())
    }

    pub fn uart_file_transfer(&self, message: UartMessage) -> Result<()> {
        let response = UartResponse::<FileTransferResponse>::try_from(message.frame)?;
        let result: Result<FileTransferResponse> = response.into();
        let mut state = self.upgrade_state.lock().unwrap();
        state.result = Some(result.map(|res| res.segment_flag as usize));
        self.cond.notify_one();
        Ok(())
    }

    pub fn mqtt_file_upgrade_status(
        &self,
        message: MqttMessage,
        mqtt_msg_sender: &mpsc::Sender<MqttMessage>,
    ) {
        let state = self.upgrade_state.lock().unwrap();
        let response = MqttFileUpgradeResponse::from(state.deref());
        mqtt_msg_sender
            .send(response.into_mqtt_message(message.to_mqtt_req_info()))
            .unwrap();
    }
}
