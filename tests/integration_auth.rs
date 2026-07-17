mod common;

use common::Server;
use redis::Commands;

#[test]
fn no_auth_required_when_no_requirepass() {
    let s = Server::spawn();
    let mut c = s.client().get_connection().unwrap();
    let _: () = c.set("k", "v").unwrap();
}

#[test]
fn auth_required_when_requirepass_set() {
    let s = Server::spawn_with_args(&["--requirepass", "topsecret"]);
    let mut c = s.client().get_connection().unwrap();

    let res: Result<String, _> = c.get("k");
    assert!(res.is_err(), "expected NOAUTH error");

    let _: () = redis::cmd("AUTH")
        .arg("topsecret")
        .query(&mut c)
        .expect("auth ok");

    let v: Option<String> = c.get("k").unwrap();
    assert!(v.is_none());
}

#[test]
fn auth_wrong_password_rejected() {
    let s = Server::spawn_with_args(&["--requirepass", "topsecret"]);
    let mut c = s.client().get_connection().unwrap();

    let res: Result<String, _> = redis::cmd("AUTH")
        .arg("wrong")
        .query(&mut c);
    assert!(res.is_err(), "AUTH with wrong password must return WRONGPASS");
}
