// Library crate for s3-turbo-list.
// Integration tests in tests/ import from here.
// The binary in src/main.rs also uses this crate via `use s3_turbo_list::...`.

// Public to integration tests (types needed to exercise pipelines):
pub mod auto_hints;
pub mod checkpoint; // CheckpointIdentity, CheckpointJournal, checkpoint_path
pub mod config;
pub mod core; // ObjectKey, ObjectProps, MatchResult, KeySpaceHints, GlobalState, DataMapContext, MonContext, S3TaskContext
pub mod data_map; // PrefixMap, ObjectMap, data_map_task
pub mod diff; // init_diff_state, diff_complete_notice
pub mod error; // FlatRuntimeError, error code constants
pub mod hints;
pub mod mon;
pub mod stats;
pub mod tasks_s3;
pub mod trace; // S3CompatEvent, S3TraceWriter, JsonlTraceWriter, create_trace_writer
pub mod utils; // AsyncParquetOutput
