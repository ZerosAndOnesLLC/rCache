mod protocol;
mod server;
mod storage;
mod command;
mod config;

use config::Config;
use server::Server;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("rcache=info".parse().unwrap()))
        .init();

    let config = Config::from_args();
    tracing::info!("Starting rCache on {}:{}", config.bind, config.port);

    let server = Server::new(config);
    if let Err(e) = server.run().await {
        tracing::error!("Server error: {}", e);
        std::process::exit(1);
    }
}
