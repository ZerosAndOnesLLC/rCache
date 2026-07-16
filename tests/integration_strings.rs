mod common;

use common::Server;
use redis::Commands;

#[test]
fn set_get_del_basics() {
    let s = Server::spawn();
    let mut c = s.client().get_connection().expect("connect");

    let _: () = c.set("k", "v").unwrap();
    let v: String = c.get("k").unwrap();
    assert_eq!(v, "v");

    let n: i64 = c.del("k").unwrap();
    assert_eq!(n, 1);

    let v: Option<String> = c.get("k").unwrap();
    assert!(v.is_none());
}

#[test]
fn incr_decr_chain() {
    let s = Server::spawn();
    let mut c = s.client().get_connection().unwrap();

    let n: i64 = c.incr("counter", 1).unwrap();
    assert_eq!(n, 1);
    let n: i64 = c.incr("counter", 5).unwrap();
    assert_eq!(n, 6);
    let n: i64 = c.decr("counter", 2).unwrap();
    assert_eq!(n, 4);
}

#[test]
fn mset_mget() {
    let s = Server::spawn();
    let mut c = s.client().get_connection().unwrap();

    let _: () = c.mset(&[("a", "1"), ("b", "2"), ("c", "3")]).unwrap();
    let vs: Vec<Option<String>> = c.mget(&["a", "b", "missing", "c"]).unwrap();
    assert_eq!(vs[0].as_deref(), Some("1"));
    assert_eq!(vs[1].as_deref(), Some("2"));
    assert!(vs[2].is_none());
    assert_eq!(vs[3].as_deref(), Some("3"));
}

#[test]
fn append_strlen_getrange() {
    let s = Server::spawn();
    let mut c = s.client().get_connection().unwrap();

    let _: () = c.set("s", "hello").unwrap();
    let n: i64 = c.append("s", ", world").unwrap();
    assert_eq!(n, 12);
    let v: String = c.get("s").unwrap();
    assert_eq!(v, "hello, world");
    let len: i64 = c.strlen("s").unwrap();
    assert_eq!(len, 12);
    let sub: String = c.getrange("s", 7, 11).unwrap();
    assert_eq!(sub, "world");
}
