// Library crate for s3-turbo-list.
// Integration tests in tests/ import from here.
// The binary in src/main.rs also uses this crate via `use s3_turbo_list::...`.

#![allow(
    clippy::collapsible_if,
    clippy::derivable_impls,
    clippy::field_reassign_with_default,
    clippy::needless_range_loop,
    clippy::new_without_default,
    clippy::result_large_err,
    clippy::should_implement_trait,
    clippy::too_many_arguments,
    clippy::type_complexity
)]

// Public to integration tests (types needed to exercise pipelines):
pub mod agent;
pub mod auto_hints;
pub mod checkpoint; // CheckpointIdentity, CheckpointJournal, checkpoint_path
pub mod compat_probe;
pub mod config;
pub mod core; // ObjectKey, ObjectProps, MatchResult, KeySpaceHints, GlobalState, DataMapContext, MonContext, S3TaskContext
pub mod data_map; // streaming list output + diff merge-join
pub mod error; // FlatRuntimeError, error code constants
pub mod filter_expr; // FilterExpr — compiled --filter expressions
pub mod hints;
pub mod local_tools;
pub mod mon;
pub mod profiles;
pub mod stats;
pub mod tasks_s3;
pub mod trace; // S3CompatEvent, S3TraceWriter, JsonlTraceWriter, create_trace_writer
pub mod utils; // AsyncParquetOutput
