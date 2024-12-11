use crate::mqtt_handler::MqttTopicType;
use crate::protocol::app_data::{ConfirmResponse, PauseMetering, RestartMetering, ResumeMetering};
use crate::protocol::AppData;
use crate::request_info::UartMessage;
use crate::service::parse_response::{mqtt_info_request_uart_handler, UartResponse};
use crate::{MqttMessage, MqttMsgHandler, Result, APP_NAME};

use crate::service::IntoMqttMessage;
use std::ops::DerefMut;
use std::sync::{mpsc, Arc, Condvar, Mutex};
use timer::{Guard, Timer};

#[derive(PartialEq)]
enum MeterState {
    Pause,
    Resume,
}

struct MeteringState {
    state: MeterState,
    resume_timer: Option<Guard>,
    result: Option<Result<()>>,
}

#[derive(Clone)]
pub struct RouteCtrl {
    timer: Arc<Timer>,
    metering_state: Arc<Mutex<MeteringState>>,
    cond: Arc<Condvar>,
    resume_interval: u32,
}

impl RouteCtrl {
    pub fn new(timer: Arc<Timer>, resume_interval: u32) -> Self {
        Self {
            timer,
            metering_state: Arc::new(Mutex::new(MeteringState {
                state: MeterState::Resume,
                resume_timer: None,
                result: None,
            })),
            cond: Arc::new(Condvar::new()),
            resume_interval,
        }
    }

    pub fn init(&self, mqtt_msg_handler: &mut MqttMsgHandler) {
        use crate::config::SCHEMA_PATH;
        use crate::schema_check;
        let topic = format!("{}{}{}", "+/set/request/", APP_NAME, "/pauseMetering");
        let schema =
            schema_check::parse_schema(SCHEMA_PATH.join("pause_metering_schema.json")).ok();
        mqtt_msg_handler.add_topic_filter(topic, MqttTopicType::PauseMetering, schema);

        let topic = format!("{}{}{}", "+/set/request/", APP_NAME, "/resumeMetering");
        let schema =
            schema_check::parse_schema(SCHEMA_PATH.join("resume_metering_schema.json")).ok();
        mqtt_msg_handler.add_topic_filter(topic, MqttTopicType::ResumeMetering, schema);

        let topic = format!("{}{}{}", "+/set/request/", APP_NAME, "/restartMetering");
        let schema =
            schema_check::parse_schema(SCHEMA_PATH.join("restart_metering_schema.json")).ok();
        mqtt_msg_handler.add_topic_filter(topic, MqttTopicType::RestartMetering, schema);
    }

    fn update_metering_state(state: &mut MeteringState, meter_state: Option<MeterState>) {
        state.state = meter_state.unwrap_or(MeterState::Resume); // TODO
                                                                 //if let Some(s) = meter_state {
                                                                 //    state.state = s;
                                                                 //}
    }

    fn add_resume_timer(
        &self,
        state: &mut MeteringState,
        uart_msg_sender: &mpsc::Sender<UartMessage>,
    ) {
        state.resume_timer = Some(self.timer.schedule_with_delay(
            chrono::Duration::seconds(self.resume_interval as i64),
            {
                let route_ctrl = self.clone();
                let uart_msg_sender = uart_msg_sender.clone();
                move || {
                    tracing::debug!("auto resume metering...");
                    // resume可能会阻塞而影响其他timer? 最好将resume timer放单独线程中... TODO
                    let res = route_ctrl.auto_resume_metering(&uart_msg_sender);
                    if let Err(e) = res {
                        tracing::error!(cause = ?e, "resume metering failed");
                    }
                }
            },
        ));
    }

    fn update_resume_timer(
        &self,
        state: &mut MeteringState,
        uart_msg_sender: &mpsc::Sender<UartMessage>,
    ) {
        if state.resume_timer.is_some() {
            self.add_resume_timer(state, uart_msg_sender);
        }
    }

    pub fn uart_response_update_resume_timer(&self, uart_msg_sender: &mpsc::Sender<UartMessage>) {
        let mut state = self.metering_state.lock().unwrap();
        self.update_resume_timer(state.deref_mut(), uart_msg_sender);
    }

    fn auto_resume_metering(&self, uart_msg_sender: &mpsc::Sender<UartMessage>) -> Result<()> {
        let mut state = self.metering_state.lock().unwrap();
        match state.state == MeterState::Resume {
            true => Ok(()),
            false => {
                mqtt_info_request_uart_handler::<ResumeMetering>(
                    ResumeMetering,
                    None,
                    uart_msg_sender,
                );
                state = self.cond.wait_while(state, |s| s.result.is_none()).unwrap();
                state.result.take().unwrap().map(|_| {
                    Self::update_metering_state(state.deref_mut(), Some(MeterState::Resume));
                })
            }
        }
    }

    pub fn auto_pause_metering(&self, uart_msg_sender: &mpsc::Sender<UartMessage>) -> Result<()> {
        let mut state = self.metering_state.lock().unwrap();
        match state.state == MeterState::Pause {
            true => {
                // 触发自动暂停 代表有抄表 广播命令等请求
                self.update_resume_timer(state.deref_mut(), uart_msg_sender);
                Ok(())
            }
            false => {
                tracing::debug!("auto pause metering...");
                mqtt_info_request_uart_handler::<PauseMetering>(
                    PauseMetering,
                    None,
                    uart_msg_sender,
                );
                state = self.cond.wait_while(state, |s| s.result.is_none()).unwrap();
                state.result.take().unwrap().map(|_| {
                    Self::update_metering_state(state.deref_mut(), Some(MeterState::Pause));
                    self.add_resume_timer(state.deref_mut(), uart_msg_sender);
                })
            }
        }
    }

    fn mqtt_operating_metering<T: Into<AppData> + Default>(
        &self,
        message: MqttMessage,
        mqtt_msg_sender: &mpsc::Sender<MqttMessage>,
        uart_msg_sender: &mpsc::Sender<UartMessage>,
        meter_state: Option<MeterState>,
    ) -> Result<()> {
        let mut state = self.metering_state.lock().unwrap();
        let result = match meter_state
            .as_ref()
            .map_or(false, |value| value == &state.state)
        {
            true => Ok(()),
            false => {
                mqtt_info_request_uart_handler::<T>(
                    T::default(),
                    Some(message.to_mqtt_req_info()),
                    uart_msg_sender,
                );
                state = self.cond.wait_while(state, |s| s.result.is_none()).unwrap();
                state.result.take().unwrap()
            }
        };

        if result.is_ok() {
            Self::update_metering_state(state.deref_mut(), meter_state);
            state.resume_timer = None;
        }

        mqtt_msg_sender
            .send(result.into_mqtt_message(message.to_mqtt_req_info()))
            .unwrap();

        Ok(())
    }

    pub fn mqtt_resume_metering(
        &self,
        message: MqttMessage,
        mqtt_msg_sender: &mpsc::Sender<MqttMessage>,
        uart_msg_sender: &mpsc::Sender<UartMessage>,
    ) -> Result<()> {
        tracing::debug!("mqtt resume metering");
        self.mqtt_operating_metering::<ResumeMetering>(
            message,
            mqtt_msg_sender,
            uart_msg_sender,
            Some(MeterState::Resume),
        )
    }

    pub fn mqtt_restart_metering(
        &self,
        message: MqttMessage,
        mqtt_msg_sender: &mpsc::Sender<MqttMessage>,
        uart_msg_sender: &mpsc::Sender<UartMessage>,
    ) -> Result<()> {
        tracing::debug!("mqtt restart metering");
        self.mqtt_operating_metering::<RestartMetering>(
            message,
            mqtt_msg_sender,
            uart_msg_sender,
            None,
        )
    }

    pub fn mqtt_pause_metering(
        &self,
        message: MqttMessage,
        mqtt_msg_sender: &mpsc::Sender<MqttMessage>,
        uart_msg_sender: &mpsc::Sender<UartMessage>,
    ) -> Result<()> {
        tracing::debug!("mqtt pause metering");
        self.mqtt_operating_metering::<PauseMetering>(
            message,
            mqtt_msg_sender,
            uart_msg_sender,
            Some(MeterState::Pause),
        )
    }

    pub fn uart_operate_metering(&self, message: UartMessage) -> Result<()> {
        let result = UartResponse::<ConfirmResponse>::try_from(message.frame)?.into();
        let mut state = self.metering_state.lock().unwrap();
        state.result = Some(result);
        self.cond.notify_one();

        Ok(())
    }
}
