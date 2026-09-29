// The library crate (src/lib.rs) holds the engine; the binary's own code —
// command line, plan, run orchestration and output — lives in src/app/.
#![allow(
    clippy::borrowed_box,
    clippy::if_same_then_else,
    clippy::too_many_arguments
)]

mod app;

fn main() {
    app::run();
}
