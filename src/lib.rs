pub mod config;
pub mod db;
pub mod git;
pub mod package;
pub use config::Config;

macro_rules! skip_error {
    ($res:expr) => {
        match $res {
            Ok(val) => val,
            Err(e) => {
                tracing::debug!("skip error: {:?}", e);
                continue;
            }
        }
    };
}

macro_rules! skip_none {
    ($res:expr) => {
        match $res {
            Some(val) => val,
            None => {
                tracing::debug!("skip none");
                continue;
            }
        }
    };
}

pub(crate) use skip_error;
pub(crate) use skip_none;
