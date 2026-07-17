mod common;

use common::Server;
use redis::Commands;

#[test]
fn list_lpush_rpush_lrange() {
    let s = Server::spawn();
    let mut c = s.client().get_connection().unwrap();

    let _: () = c.rpush("l", &["a", "b", "c"]).unwrap();
    let _: () = c.lpush("l", "z").unwrap();
    let items: Vec<String> = c.lrange("l", 0, -1).unwrap();
    assert_eq!(items, vec!["z", "a", "b", "c"]);
}

#[test]
fn hash_hset_hget_hgetall() {
    let s = Server::spawn();
    let mut c = s.client().get_connection().unwrap();

    let _: () = c.hset_multiple("h", &[("a", "1"), ("b", "2")]).unwrap();
    let v: String = c.hget("h", "a").unwrap();
    assert_eq!(v, "1");

    let all: std::collections::HashMap<String, String> = c.hgetall("h").unwrap();
    assert_eq!(all.get("a").map(String::as_str), Some("1"));
    assert_eq!(all.get("b").map(String::as_str), Some("2"));
}

#[test]
fn set_sadd_smembers_srem() {
    let s = Server::spawn();
    let mut c = s.client().get_connection().unwrap();

    let _: () = c.sadd("s", &["a", "b", "c"]).unwrap();
    let mut members: Vec<String> = c.smembers("s").unwrap();
    members.sort();
    assert_eq!(members, vec!["a", "b", "c"]);

    let removed: i64 = c.srem("s", "b").unwrap();
    assert_eq!(removed, 1);
}

#[test]
fn zset_zadd_zrange_zscore() {
    let s = Server::spawn();
    let mut c = s.client().get_connection().unwrap();

    let _: () = c.zadd_multiple("z", &[(1.0, "a"), (2.0, "b"), (3.0, "c")]).unwrap();
    let items: Vec<String> = c.zrange("z", 0, -1).unwrap();
    assert_eq!(items, vec!["a", "b", "c"]);

    let score: f64 = c.zscore("z", "b").unwrap();
    assert_eq!(score, 2.0);
}

#[test]
fn expire_and_ttl() {
    let s = Server::spawn();
    let mut c = s.client().get_connection().unwrap();

    let _: () = c.set("k", "v").unwrap();
    let _: bool = c.expire("k", 100).unwrap();
    let ttl: i64 = c.ttl("k").unwrap();
    assert!(ttl > 0 && ttl <= 100, "ttl in (0, 100]; got {ttl}");

    let _: bool = c.persist("k").unwrap();
    let ttl: i64 = c.ttl("k").unwrap();
    assert_eq!(ttl, -1, "persist should clear ttl");
}
