use clap::Error;
use std::env;
use stellar_archivist::cli;

#[tokio::main]
async fn main() {
    if let Err(e) = cli::run(env::args_os()).await {
        match e {
            cli::Error::Clap(e) => e.exit(),
             _ => Error::raw(clap::error::ErrorKind::ValueValidation, e).exit(),
        }
    }
}
