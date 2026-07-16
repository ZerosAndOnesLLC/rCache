mod common;

use common::Server;
use redis::Commands;

#[test]
fn multi_exec_atomic_commit() {
    let s = Server::spawn();
    let mut c = s.client().get_connection().unwrap();

    let (a, b): (i64, i64) = redis::pipe()
        .atomic()
        .incr("counter", 3)
        .incr("counter", 4)
        .query(&mut c)
        .unwrap();
    assert_eq!((a, b), (3, 7));
    let final_val: i64 = c.get("counter").unwrap();
    assert_eq!(final_val, 7);
}

#[test]
fn watch_aborts_on_mutation() {
    let s = Server::spawn();
    let mut tx = s.client().get_connection().unwrap();
    let mut other = s.client().get_connection().unwrap();

    let _: () = tx.set("balance", 100i64).unwrap();

    // WATCH balance, then read it
    let _: () = redis::cmd("WATCH").arg("balance").query(&mut tx).unwrap();
    let v: i64 = tx.get("balance").unwrap();
    assert_eq!(v, 100);

    // Another connection mutates the watched key
    let _: () = other.set("balance", 200i64).unwrap();

    // MULTI/EXEC must report abort (nil reply)
    let res: redis::Value = redis::pipe()
        .atomic()
        .cmd("SET")
        .arg("balance")
        .arg(v + 10)
        .query(&mut tx)
        .unwrap();
    assert!(matches!(res, redis::Value::Nil), "expected nil from aborted EXEC, got {:?}", res);

    let final_val: i64 = other.get("balance").unwrap();
    assert_eq!(final_val, 200, "watched mutation should be preserved");
}

#[test]
fn watch_does_not_abort_without_mutation() {
    let s = Server::spawn();
    let mut tx = s.client().get_connection().unwrap();

    let _: () = tx.set("x", 1i64).unwrap();
    let _: () = redis::cmd("WATCH").arg("x").query(&mut tx).unwrap();
    let v: i64 = tx.get("x").unwrap();

    let res: redis::Value = redis::pipe()
        .atomic()
        .cmd("SET")
        .arg("x")
        .arg(v + 1)
        .query(&mut tx)
        .unwrap();
    assert!(!matches!(res, redis::Value::Nil), "EXEC should succeed");

    let final_val: i64 = tx.get("x").unwrap();
    assert_eq!(final_val, 2);
}
