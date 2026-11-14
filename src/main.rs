// The library crate (src/lib.rs) holds the engine; the binary's own code —
// command line, result contract and run orchestration — lives in src/app/.
mod app;

fn main() {
    std::process::exit(app::main());
}
