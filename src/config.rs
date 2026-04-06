/// Server configuration.
#[derive(Debug, Clone)]
pub struct Config {
    pub bind: String,
    pub port: u16,
    pub databases: usize,
    pub maxclients: usize,
    pub requirepass: Option<String>,
    pub hz: u64,
    pub maxmemory: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            bind: "0.0.0.0".to_string(),
            port: 6379,
            databases: 16,
            maxclients: 10000,
            requirepass: None,
            hz: 10,
            maxmemory: 0, // 0 = no limit
        }
    }
}

impl Config {
    pub fn from_args() -> Self {
        let mut config = Self::default();
        let args: Vec<String> = std::env::args().collect();
        let mut i = 1;
        while i < args.len() {
            match args[i].as_str() {
                "--bind" => {
                    i += 1;
                    if i < args.len() {
                        config.bind = args[i].clone();
                    }
                }
                "--port" => {
                    i += 1;
                    if i < args.len() {
                        config.port = args[i].parse().unwrap_or(6379);
                    }
                }
                "--databases" => {
                    i += 1;
                    if i < args.len() {
                        config.databases = args[i].parse().unwrap_or(16);
                    }
                }
                "--maxclients" => {
                    i += 1;
                    if i < args.len() {
                        config.maxclients = args[i].parse().unwrap_or(10000);
                    }
                }
                "--requirepass" => {
                    i += 1;
                    if i < args.len() {
                        config.requirepass = Some(args[i].clone());
                    }
                }
                _ => {}
            }
            i += 1;
        }
        config
    }
}
