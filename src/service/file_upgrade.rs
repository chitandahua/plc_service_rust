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
    RunningStatusRequest, RunningStatusResponse, FILE_CHECK_ERROR, FILE_TRANSFER_PREFIX_LEN,
};
use crate::protocol::{Frame, FRAME_SIZE, USER_DATA_PREFIX_SIZE};
use crate::request_info::{FrameKey, ReqInfo, UartMessage};
use crate::service::module_info::MODULE_INFO;
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
    OtherError = 255,
}

#[derive(Debug, Default)]
struct FileUpgradeState {
    status: UpgradeStatus,
    start_time: NaiveDateTime,
    end_time: NaiveDateTime,
    upgrade_result: UpgradeResult,
    result: Option<Result<usize>>,
    upgrading: bool,
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
    fn start_upgrade(&mut self) {
        self.upgrading = true;
        self.status = UpgradeStatus::Idle;
        self.start_time = chrono::Local::now().naive_local(); // TODO 等待传输完成才开始？
    }

    fn finish_upgrade(&mut self, upgrate_result: UpgradeResult) {
        self.status = UpgradeStatus::Finished;
        self.end_time = chrono::Local::now().naive_local();
        self.upgrade_result = upgrate_result;
        self.upgrading = false;
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

        let request: MqttFileUpgradeRequest = serde_json::from_str(message.payload())?;
        // 读文件到buffer中
        let content = match File::open(&request.file_path) {
            Ok(mut file) => {
                mqtt_msg_sender
                    .send(().into_mqtt_message(message.to_mqtt_req_info()))
                    .unwrap();
                let mut content = Vec::new();
                file.read_to_end(&mut content).unwrap();
                content
            }
            Err(e) => {
                tracing::error!(cause = ?e, "open file {} failed", request.file_path);
                mqtt_msg_sender
                    .send(
                        anyhow::anyhow!(MqttResponseError::InvalidUpgradeFile)
                            .into_mqtt_message(message.to_mqtt_req_info()),
                    )
                    .unwrap();
                return Ok(());
            }
        };

        let file_flag = match request.flag {
            0 => FileFlag::Module,
            1 => FileFlag::MasterAndSlave,
            2 => FileFlag::Slave,
            _ => unreachable!(),
        };

        // 传输文件
        {
            let mut state = self.upgrade_state.lock().unwrap();
            if state.upgrading {
                mqtt_msg_sender
                    .send(
                        anyhow::anyhow!(MqttResponseError::AlreadyUpgrading)
                            .into_mqtt_message(message.to_mqtt_req_info()),
                    )
                    .unwrap();
                return Ok(());
            }
            state.start_upgrade();

            // TODO 若有地址域 则需要加上地址域长度
            const PREFIX_LEN: usize = FRAME_SIZE + USER_DATA_PREFIX_SIZE + FILE_TRANSFER_PREFIX_LEN;
            let file_data_size: usize =
                MODULE_INFO.get().unwrap().max_packet_per_packet as usize - PREFIX_LEN;
            //const FILE_DATA_SIZE: usize = 128;
            let segment_num = content.len().div_ceil(file_data_size);

            for (segment_offset, file_data) in content.chunks(file_data_size).enumerate() {
                // TODO 子模块升级需要设置通信模块标识以及地址域？
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

                let mut upgrade_result = UpgradeResult::TransferFailed;
                let result = state.result.take().unwrap().and_then(|segment| {
                    if segment as u32 == FILE_CHECK_ERROR {
                        upgrade_result = UpgradeResult::FileError;
                        Err(anyhow::anyhow!("file check error"))
                    } else if segment != segment_offset {
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
                    state.finish_upgrade(upgrade_result);
                    return Ok(());
                }
            }
        }

        thread_pool.execute({
            let file_upgrade = self.clone();
            let uart_msg_sender = uart_msg_sender.clone();
            let upgrade_wait_time = MODULE_INFO.get().unwrap().upgrade_wait_time as u64;
            move || {
                let mut state = file_upgrade.upgrade_state.lock().unwrap();
                const WAIT_TIME: u64 = 30;
                let wait_total_count = upgrade_wait_time.div_ceil(WAIT_TIME);
                let mut wait_count = 0;

                state.status = UpgradeStatus::Upgrading;
                while state.status == UpgradeStatus::Upgrading && wait_count < wait_total_count {
                    let wait_time = if wait_count == wait_total_count - 1 {
                        upgrade_wait_time - wait_count * WAIT_TIME
                    } else {
                        WAIT_TIME
                    };
                    wait_count += 1;
                    let duration = Duration::from_secs(wait_time);
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

                if wait_count == wait_total_count {
                    tracing::error!("file upgrade timeout {} s", upgrade_wait_time);
                    state.finish_upgrade(UpgradeResult::OtherError);
                } else {
                    state.finish_upgrade(UpgradeResult::Success);
                }
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
