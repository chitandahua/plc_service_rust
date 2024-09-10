use clap::Parser;
use plc_service::{Args, PlcService, Result};

use tracing::{debug, error};
use tracing_subscriber::fmt::time::OffsetTime;
use tracing_subscriber::{self, EnvFilter};

fn main() {
    let args = Args::parse();

    if args.ver {
        plc_service::get_version_info();
        return;
    }

    init_log(&args).expect("init log failed");

    let result = run(args);
    match result {
        Ok(_) => {}
        Err(e) => {
            error!(cause = ?e, "app run failed");
        }
    }
}

fn get_log_level(log_level: &str) -> String {
    if log_level == "debug" || log_level == "trace" {
        format!("paho_mqtt=info,paho_mqtt_c=info,{}", log_level)
    } else {
        log_level.to_string()
    }
}

fn init_log(args: &Args) -> Result<()> {
    debug!("init log");
    let identity = std::ffi::CStr::from_bytes_with_nul(b"JDIoT\0").unwrap();
    let (options, facility) = Default::default();

    let filter_layer = args
        .log_level
        .as_ref()
        .map(|level| EnvFilter::try_new(get_log_level(level)).map_err(|_| ()))
        .unwrap_or_else(|| EnvFilter::try_from_default_env().map_err(|_| ()))
        .or_else(|_| EnvFilter::try_new(get_log_level("debug")).map_err(|_| ()))
        .expect("new env filter failed");

    match args.syslog {
        true => {
            tracing_subscriber::fmt()
                .with_timer(OffsetTime::local_rfc_3339().expect("could not get local time offset"))
                .with_env_filter(filter_layer)
                .with_writer(syslog_tracing::Syslog::new(identity, options, facility).unwrap())
                .try_init()
                .expect("init subscriber failed");
        }
        false => {
            tracing_subscriber::fmt()
                .with_timer(OffsetTime::local_rfc_3339().expect("could not get local time offset"))
                .with_env_filter(filter_layer)
                .try_init()
                .expect("init subscriber failed");
        }
    };

    Ok(())
}

fn run(args: Args) -> Result<()> {
    debug!("app start");
    let app = PlcService::new(args)?;
    app.run()
}
