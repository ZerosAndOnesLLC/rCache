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
    pub maxmemory_policy: String,
    pub maxmemory_samples: usize,
    pub rdb_filename: String,
    pub aof_enabled: bool,
    pub aof_filename: String,
    pub appendfsync: String,
    /// LFU logarithmic factor -- higher values slow counter growth.
    pub lfu_log_factor: u64,
    /// LFU decay time in minutes -- counter decremented every N minutes of inactivity.
    pub lfu_decay_time: u64,
    /// Optional HTTP/REST API port. None = disabled.
    pub http_port: Option<u16>,
    /// Optional TLS port. If set, a TLS listener is started on this port.
    pub tls_port: Option<u16>,
    /// Path to the TLS certificate file (PEM format).
    pub tls_cert_file: Option<String>,
    /// Path to the TLS private key file (PEM format).
    pub tls_key_file: Option<String>,
    /// Compression: minimum byte size to trigger compression.
    pub compression_threshold: usize,
    /// Whether transparent compression is enabled.
    pub compression_enabled: bool,
    /// Slowlog: minimum execution time in microseconds to log (default 10000).
    pub slowlog_log_slower_than: i64,
    /// Maximum number of slow log entries to keep.
    pub slowlog_max_len: usize,
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
            maxmemory_policy: "noeviction".to_string(),
            maxmemory_samples: 5,
            rdb_filename: "dump.rdb".to_string(),
            aof_enabled: false,
            aof_filename: "appendonly.aof".to_string(),
            appendfsync: "everysec".to_string(),
            lfu_log_factor: 10,
            lfu_decay_time: 1,
            http_port: None,
            tls_port: None,
            tls_cert_file: None,
            tls_key_file: None,
            compression_threshold: 1024,
            compression_enabled: false,
            slowlog_log_slower_than: 10000,
            slowlog_max_len: 128,
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
                "--maxmemory-policy" => {
                    i += 1;
                    if i < args.len() {
                        config.maxmemory_policy = args[i].clone();
                    }
                }
                "--maxmemory-samples" => {
                    i += 1;
                    if i < args.len() {
                        config.maxmemory_samples = args[i].parse().unwrap_or(5);
                    }
                }
                "--dbfilename" => {
                    i += 1;
                    if i < args.len() {
                        config.rdb_filename = args[i].clone();
                    }
                }
                "--appendonly" => {
                    i += 1;
                    if i < args.len() {
                        config.aof_enabled = args[i] == "yes";
                    }
                }
                "--appendfilename" => {
                    i += 1;
                    if i < args.len() {
                        config.aof_filename = args[i].clone();
                    }
                }
                "--appendfsync" => {
                    i += 1;
                    if i < args.len() {
                        config.appendfsync = args[i].clone();
                    }
                }
                "--maxmemory" => {
                    i += 1;
                    if i < args.len() {
                        config.maxmemory = args[i].parse().unwrap_or(0);
                    }
                }
                "--lfu-log-factor" => {
                    i += 1;
                    if i < args.len() {
                        config.lfu_log_factor = args[i].parse().unwrap_or(10);
                    }
                }
                "--lfu-decay-time" => {
                    i += 1;
                    if i < args.len() {
                        config.lfu_decay_time = args[i].parse().unwrap_or(1);
                    }
                }
                "--http-port" => {
                    i += 1;
                    if i < args.len() {
                        config.http_port = args[i].parse().ok();
                    }
                }
                "--tls-port" => {
                    i += 1;
                    if i < args.len() {
                        config.tls_port = args[i].parse().ok();
                    }
                }
                "--tls-cert-file" => {
                    i += 1;
                    if i < args.len() {
                        config.tls_cert_file = Some(args[i].clone());
                    }
                }
                "--tls-key-file" => {
                    i += 1;
                    if i < args.len() {
                        config.tls_key_file = Some(args[i].clone());
                    }
                }
                "--compression-enabled" => {
                    i += 1;
                    if i < args.len() {
                        config.compression_enabled = args[i] == "yes";
                    }
                }
                "--compression-threshold" => {
                    i += 1;
                    if i < args.len() {
                        config.compression_threshold = args[i].parse().unwrap_or(1024);
                    }
                }
                "--slowlog-log-slower-than" => {
                    i += 1;
                    if i < args.len() {
                        config.slowlog_log_slower_than = args[i].parse().unwrap_or(10000);
                    }
                }
                "--slowlog-max-len" => {
                    i += 1;
                    if i < args.len() {
                        config.slowlog_max_len = args[i].parse().unwrap_or(128);
                    }
                }
                _ => {}
            }
            i += 1;
        }
        config
    }
}
