// Pedantic lints accepted for the binary's perf wiring: `doc_markdown` and
// `cast_possible_truncation` mirror the workspace policy in lib.rs;
// `struct_field_names` (the Session baseline fields share a `_start` suffix) and
// `unused_self` (the feature-off no-op `Session::report`) are perf-module local.
#![allow(clippy::doc_markdown)]
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::struct_field_names)]
#![allow(clippy::unused_self)]

mod perf;

use clap::Error;
use std::env;
use stellar_archivist::cli;

#[tokio::main]
async fn main() {
    // Feature-gated perf instrumentation; a no-op without `--features perf-metrics`.
    let perf = perf::start();

    match cli::run(env::args_os()).await {
        Ok(()) => perf.report(),
        // clap handled --help / --version / a bad argument: let it print its own
        // message and set its own exit code, with no perf output polluting it.
        Err(cli::Error::Clap(e)) => e.exit(),
        Err(e) => {
            perf.report();
            Error::raw(clap::error::ErrorKind::ValueValidation, e).exit();
        }
    }
}
