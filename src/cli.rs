use clap::Parser;
use lazy_static::lazy_static;

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
    #[arg(short)]
    pub syslog: bool,
}
