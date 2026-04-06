use bytes::Bytes;
use crate::server::PubSubManager;

/// Determine the event-type flag for a given command name.
/// Returns None if the command doesn't map to a notification category.
fn event_flag(cmd: &str) -> Option<char> {
    match cmd {
        // g = generic commands
        "DEL" | "UNLINK" | "EXPIRE" | "PEXPIRE" | "EXPIREAT" | "PEXPIREAT"
        | "RENAME" | "RENAMENX" | "COPY" | "PERSIST" | "FLUSHDB" | "FLUSHALL"
        | "SORT" | "SORT_RO" | "MOVE" | "RESTORE" => Some('g'),

        // $ = string commands
        "SET" | "SETNX" | "SETEX" | "PSETEX" | "MSET" | "MSETNX"
        | "APPEND" | "SETRANGE" | "INCR" | "DECR" | "INCRBY" | "DECRBY"
        | "INCRBYFLOAT" | "GETSET" | "GETDEL" | "GETEX"
        | "SETBIT" | "BITOP" | "BITFIELD" => Some('$'),

        // l = list commands
        "LPUSH" | "RPUSH" | "LPUSHX" | "RPUSHX" | "LPOP" | "RPOP"
        | "LSET" | "LINSERT" | "LREM" | "LTRIM" | "LMOVE" | "LMPOP"
        | "BLPOP" | "BRPOP" | "BLMOVE" | "BLMPOP" => Some('l'),

        // s = set commands
        "SADD" | "SREM" | "SPOP" | "SMOVE"
        | "SDIFFSTORE" | "SINTERSTORE" | "SUNIONSTORE" => Some('s'),

        // h = hash commands
        "HSET" | "HSETNX" | "HDEL" | "HINCRBY" | "HINCRBYFLOAT" => Some('h'),

        // z = sorted set commands
        "ZADD" | "ZREM" | "ZINCRBY" | "ZPOPMIN" | "ZPOPMAX"
        | "ZUNIONSTORE" | "ZINTERSTORE" | "ZDIFFSTORE"
        | "ZRANGESTORE" | "ZMPOP" | "BZPOPMIN" | "BZPOPMAX" | "BZMPOP" => Some('z'),

        // t = stream commands
        "XADD" | "XDEL" | "XTRIM" | "XGROUP" | "XCLAIM" | "XAUTOCLAIM" => Some('t'),

        _ => None,
    }
}

/// Check whether a given event-type flag is enabled by the configuration string.
///
/// The config string may contain:
///   K = publish keyspace events
///   E = publish keyevent events
///   g$ l s h z x e t = individual event categories
///   A = alias for g$lshzxet (all categories)
///
/// An event is published only if:
///   1. At least K or E is present (otherwise nothing is published).
///   2. The specific category flag (g, $, l, etc.) is present (or A).
fn is_event_enabled(config: &str, flag: char) -> bool {
    if config.is_empty() {
        return false;
    }
    // Must have at least K or E
    if !config.contains('K') && !config.contains('E') {
        return false;
    }
    // Check for the specific flag or 'A' (all)
    if config.contains('A') {
        // A expands to g$lshzxet
        return "g$lshzxet".contains(flag);
    }
    config.contains(flag)
}

/// Send keyspace/keyevent notifications for a command that affected a key.
///
/// This should be called after a successful write command. It checks the config
/// flags and publishes to the appropriate channels via PubSubManager.
///
/// - `pubsub`: the PubSubManager to publish through
/// - `config_flags`: the current `notify-keyspace-events` config string
/// - `db_index`: the database index the key belongs to
/// - `event`: the event name (typically the lowercase command name, e.g. "set", "del")
/// - `key`: the affected key
pub fn notify(
    pubsub: &PubSubManager,
    config_flags: &str,
    db_index: usize,
    event: &str,
    key: &Bytes,
) {
    // Fast path: if config is empty, no notifications at all
    if config_flags.is_empty() {
        return;
    }

    // Determine which flag this event corresponds to
    let flag = match event {
        "expired" => 'x',
        "evicted" => 'e',
        _ => {
            // Look up by command name (uppercase)
            let upper = event.to_uppercase();
            match event_flag(&upper) {
                Some(f) => f,
                None => return,
            }
        }
    };

    if !is_event_enabled(config_flags, flag) {
        return;
    }

    let event_bytes = Bytes::from(event.to_string());

    // K flag: publish on __keyspace@<db>__:<key> with the event name as message
    if config_flags.contains('K') {
        let channel = Bytes::from(format!("__keyspace@{}__:{}", db_index, String::from_utf8_lossy(key)));
        pubsub.publish(&channel, &event_bytes);
    }

    // E flag: publish on __keyevent@<db>__:<event> with the key as message
    if config_flags.contains('E') {
        let channel = Bytes::from(format!("__keyevent@{}__:{}", db_index, event));
        pubsub.publish(&channel, key);
    }
}

/// Convenience: derive the event name from a Redis command name.
/// Returns the lowercase command name, which is what Redis uses as the event name.
pub fn event_name_for_command(cmd: &str) -> &str {
    match cmd {
        // Some commands have special event names in Redis
        "SETNX" => "set",
        "SETEX" => "set",
        "PSETEX" => "set",
        "LPUSHX" => "lpush",
        "RPUSHX" => "rpush",
        _ => "", // caller should use cmd.to_lowercase()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_flag() {
        assert_eq!(event_flag("SET"), Some('$'));
        assert_eq!(event_flag("DEL"), Some('g'));
        assert_eq!(event_flag("LPUSH"), Some('l'));
        assert_eq!(event_flag("SADD"), Some('s'));
        assert_eq!(event_flag("HSET"), Some('h'));
        assert_eq!(event_flag("ZADD"), Some('z'));
        assert_eq!(event_flag("XADD"), Some('t'));
        assert_eq!(event_flag("GET"), None);
        assert_eq!(event_flag("PING"), None);
    }

    #[test]
    fn test_is_event_enabled() {
        // Empty config: nothing enabled
        assert!(!is_event_enabled("", '$'));

        // No K or E: nothing published even with flags
        assert!(!is_event_enabled("g$", '$'));

        // K with $ enabled
        assert!(is_event_enabled("K$", '$'));
        assert!(!is_event_enabled("K$", 'g'));

        // E with g enabled
        assert!(is_event_enabled("Eg", 'g'));
        assert!(!is_event_enabled("Eg", '$'));

        // A = all categories
        assert!(is_event_enabled("KA", '$'));
        assert!(is_event_enabled("KA", 'g'));
        assert!(is_event_enabled("KA", 'l'));
        assert!(is_event_enabled("KA", 's'));
        assert!(is_event_enabled("KA", 'h'));
        assert!(is_event_enabled("KA", 'z'));
        assert!(is_event_enabled("KA", 'x'));
        assert!(is_event_enabled("KA", 'e'));
        assert!(is_event_enabled("KA", 't'));

        // EA = all categories with keyevent
        assert!(is_event_enabled("EA", 'g'));

        // KEA = both keyspace and keyevent, all categories
        assert!(is_event_enabled("KEA", '$'));
    }

    #[test]
    fn test_notify_empty_config() {
        // Should not panic and return immediately
        let pubsub = PubSubManager::new();
        notify(&pubsub, "", 0, "set", &Bytes::from("mykey"));
    }

    #[test]
    fn test_notify_publishes_keyspace() {
        let mut pubsub = PubSubManager::new();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

        // Subscribe to keyspace channel
        let channel = Bytes::from("__keyspace@0__:mykey");
        pubsub.subscribe(1, channel, tx);

        notify(&pubsub, "K$", 0, "set", &Bytes::from("mykey"));

        // Should have received a message
        let msg = rx.try_recv();
        assert!(msg.is_ok());
    }

    #[test]
    fn test_notify_publishes_keyevent() {
        let mut pubsub = PubSubManager::new();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

        // Subscribe to keyevent channel
        let channel = Bytes::from("__keyevent@0__:set");
        pubsub.subscribe(1, channel, tx);

        notify(&pubsub, "E$", 0, "set", &Bytes::from("mykey"));

        let msg = rx.try_recv();
        assert!(msg.is_ok());
    }

    #[test]
    fn test_notify_disabled_flag() {
        let mut pubsub = PubSubManager::new();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

        let channel = Bytes::from("__keyspace@0__:mykey");
        pubsub.subscribe(1, channel, tx);

        // K is set but only 'g' category, not '$' (string) - SET should not fire
        notify(&pubsub, "Kg", 0, "set", &Bytes::from("mykey"));

        let msg = rx.try_recv();
        assert!(msg.is_err()); // no message
    }

    #[test]
    fn test_notify_expired_event() {
        let mut pubsub = PubSubManager::new();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

        let channel = Bytes::from("__keyevent@0__:expired");
        pubsub.subscribe(1, channel, tx);

        notify(&pubsub, "Ex", 0, "expired", &Bytes::from("mykey"));

        let msg = rx.try_recv();
        assert!(msg.is_ok());
    }

    #[test]
    fn test_notify_evicted_event() {
        let mut pubsub = PubSubManager::new();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

        let channel = Bytes::from("__keyevent@0__:evicted");
        pubsub.subscribe(1, channel, tx);

        notify(&pubsub, "Ee", 0, "evicted", &Bytes::from("mykey"));

        let msg = rx.try_recv();
        assert!(msg.is_ok());
    }
}
