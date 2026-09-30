//! The binary: everything `main` runs, split by concern. Each module sees
//! the others' items (and these imports) through `use super::*`.

use chrono::Local;
use clap::{Args, CommandFactory, Parser, Subcommand, ValueEnum};
use clap_complete::Shell;
use config::S3TurboConfig;
use core::RunMode;
use log::{error, info, warn};
use s3_turbo_list::{
    agent, checkpoint, compat_probe, config, core, data_map, hints, local_tools, mon, profiles,
    startup, tasks_s3, trace,
};
use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

mod cli;
mod exit;
mod local;
mod outputs;
mod plan;
mod run;

pub(crate) use cli::*;
pub(crate) use exit::*;
pub(crate) use local::*;
pub(crate) use outputs::*;
pub(crate) use plan::*;
pub(crate) use run::*;
