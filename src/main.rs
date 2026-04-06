mod protocol;
mod server;
mod storage;
mod command;
mod config;
pub mod persistence;

use config::Config;
use server::Server;
use storage::Store;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("rcache=info".parse().unwrap()))
        .init();

    let config = Config::from_args();
    tracing::info!("Starting rCache on {}:{}", config.bind, config.port);

    let store = load_on_startup(&config);

    let server = Server::new(config, store);
    if let Err(e) = server.run().await {
        tracing::error!("Server error: {}", e);
        std::process::exit(1);
    }
}

fn load_on_startup(config: &Config) -> Store {
    let aof_path = std::path::Path::new(&config.aof_filename);
    let rdb_path = std::path::Path::new(&config.rdb_filename);

    if config.aof_enabled && aof_path.exists() {
        tracing::info!("Loading data from AOF file: {}", config.aof_filename);
        let mut store = Store::new(config.databases);
        match persistence::aof::replay(aof_path, &mut store) {
            Ok(count) => {
                tracing::info!("AOF replay complete: {} commands replayed", count);
                return store;
            }
            Err(e) => {
                tracing::error!("Failed to replay AOF: {}", e);
            }
        }
    }

    if rdb_path.exists() {
        tracing::info!("Loading data from RDB file: {}", config.rdb_filename);
        match persistence::rdb::load(rdb_path, config.databases) {
            Ok(store) => {
                let total_keys: usize = (0..store.db_count()).map(|i| store.db(i).len()).sum();
                tracing::info!("RDB load complete: {} keys loaded", total_keys);
                return store;
            }
            Err(e) => {
                tracing::error!("Failed to load RDB: {}", e);
            }
        }
    }

    tracing::info!("Starting with empty dataset");
    Store::new(config.databases)
}
