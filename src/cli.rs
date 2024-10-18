use clap::Parser;
use lazy_static::lazy_static;
use std::{net::SocketAddr, path::PathBuf};

lazy_static! {
    static ref VERSION: &'static str =
        option_env!("VERGEN_GIT_SEMVER_LIGHTWEIGHT").unwrap_or(env!("VERGEN_BUILD_SEMVER"));
    static ref LONG_VERSION: String = format!(
        "
Build Timestamp:     {}
Build Version:       {}
Commit SHA:          {:?}
Commit Date:         {:?}
Commit Branch:       {:?}
",
        env!("VERGEN_BUILD_TIMESTAMP"),
        env!("VERGEN_BUILD_SEMVER"),
        option_env!("VERGEN_GIT_SHA"),
        option_env!("VERGEN_GIT_COMMIT_TIMESTAMP"),
        option_env!("VERGEN_GIT_BRANCH"),
    );
}

#[derive(Parser, Debug)]
pub struct ConcurrentLimit {
    /// Concurrency limit
    #[arg(short = 'c')]
    pub concurrency: Option<usize>,

    /// Number of requests per second
    #[arg(short = 'r', value_name = "NUM:PER")]
    pub requests_per_second: Option<String>,

    /// Concurrent response timeout (s)
    #[arg(short = 't')]
    pub timeout: Option<u32>,
}

#[derive(Parser, Debug)]
pub struct MeterReading {
    /// Queue size per meter address
    #[arg(short = 'q')]
    pub queue_size: Option<usize>,

    /// Max concurrent address number
    #[arg(short = 'm')]
    pub max_addr_num: Option<usize>,

    /// Cache queue aging time (min)
    #[arg(short = 'a')]
    pub aging_time: Option<u32>,
}

#[derive(Parser, Debug)]
#[clap(
    about,
    version(*VERSION),
    long_version(LONG_VERSION.as_str()),
)]
#[command(override_usage = "PLCService [OPTIONS]")]
pub struct Args {
    /// log level
    #[arg(short, value_parser=["trace", "debug", "info", "warn", "error"])]
    pub log_level: Option<String>,

    #[arg(short)]
    /// print version and compile time
    pub ver: bool,

    /// output log to syslog
    #[arg(long)]
    pub syslog: bool,

    /// save node data to file instead of sqlite database
    #[arg(short, value_parser = clap::value_parser!(PathBuf))]
    pub file: Option<PathBuf>,

    /// init timeout(s)
    #[arg(short)]
    pub init_timeout: Option<u32>,

    /// uart request timeout(ms)
    #[arg(short)]
    pub uart_timeout: Option<u32>,

    /// uart spy address (ip:port)
    #[arg(short = 's', value_parser = clap::value_parser!(SocketAddr))]
    pub tcp_addr: Option<SocketAddr>,

    #[command(flatten)]
    pub concurrent_limit: ConcurrentLimit,

    #[command(flatten)]
    pub meter_reading: MeterReading,

    /// metering auto resume interval
    #[arg(long)]
    pub resume: Option<u32>,
}
