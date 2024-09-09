use crate::PlcInit;
use std::path::PathBuf;
use std::sync::Arc;
use std::{
    marker::PhantomData,
    sync::atomic::{AtomicBool, AtomicU8},
    thread::{self, JoinHandle},
};

// 定义状态
struct Init;
struct CheckMid;
struct Running;
struct Destroy;

// Plc设备状态机
struct PlcDeviceState<S> {
    _state: PhantomData<S>,
}

impl PlcDeviceState<Init> {
    fn new() -> Self {
        PlcDeviceState {
            _state: PhantomData,
        }
    }

    fn check_mid(self) -> Result<PlcDeviceState<CheckMid>, PlcDeviceState<Init>> {
        Ok(PlcDeviceState {
            _state: PhantomData,
        })
    }
}

impl PlcDeviceState<CheckMid> {
    fn run(self) -> Result<PlcDeviceState<Running>, PlcDeviceState<Destroy>> {
        Ok(PlcDeviceState {
            _state: PhantomData,
        })
    }
}

impl PlcDeviceState<Running> {
    fn execute(self) -> Result<PlcDeviceState<Running>, PlcDeviceState<Destroy>> {
        //thread::sleep(std::time::Duration::from_mins(10));
        Ok(PlcDeviceState {
            _state: PhantomData,
        })
    }
}

impl PlcDeviceState<Destroy> {
    fn reinit(self) -> Result<PlcDeviceState<Init>, PlcDeviceState<Destroy>> {
        Ok(PlcDeviceState {
            _state: PhantomData,
        })
    }
}

pub struct PlcDevice {
    port: PathBuf,
    online: Arc<AtomicBool>,
    plc_init: Arc<PlcInit>,
    consecutive_timeouts: Arc<AtomicU8>,
}

impl PlcDevice {
    pub fn new(port: PathBuf, plc_init: Arc<PlcInit>) -> Self {
        PlcDevice {
            port,
            online: Arc::new(AtomicBool::new(false)),
            plc_init,
            consecutive_timeouts: Arc::new(AtomicU8::new(0)),
        }
    }

    pub fn run(self) -> crate::Result<JoinHandle<()>> {
        let handler = thread::spawn(move || {
            let mut init_state = PlcDeviceState::<Init>::new();
            'outer: loop {
                let check_mid_state = match init_state.check_mid() {
                    Ok(state) => state,
                    Err(state) => {
                        init_state = state;
                        continue 'outer;
                    }
                };

                let mut running_state = match check_mid_state.run() {
                    Ok(state) => state,
                    Err(state) => {
                        let mut destroy_state = state;
                        loop {
                            match destroy_state.reinit() {
                                Ok(state) => {
                                    init_state = state;
                                    continue 'outer;
                                }
                                Err(state) => destroy_state = state,
                            };
                        }
                    }
                };

                loop {
                    match running_state.execute() {
                        Ok(state) => running_state = state,
                        Err(state) => loop {
                            let mut destroy_state = state;
                            loop {
                                match destroy_state.reinit() {
                                    Ok(state) => {
                                        init_state = state;
                                        continue 'outer;
                                    }
                                    Err(state) => destroy_state = state,
                                };
                            }
                        },
                    }
                }
            }
        });

        Ok(handler)
    }
}
