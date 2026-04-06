use bytes::{Bytes, BytesMut, BufMut};

/// RESP protocol value types.
#[derive(Debug, Clone, PartialEq)]
pub enum RespValue {
    // RESP2 types
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Bytes),
    Array(Vec<RespValue>),
    Null,
    NullArray,
    // RESP3 types
    Double(f64),                          // ,3.14\r\n
    Boolean(bool),                        // #t\r\n or #f\r\n
    BlobError(Bytes),                     // !<len>\r\n<data>\r\n
    VerbatimString(Bytes),                // =<len>\r\n<encoding>:<data>\r\n
    BigNumber(String),                    // (12345\r\n
    Map(Vec<(RespValue, RespValue)>),     // %N\r\n
    RespSet(Vec<RespValue>),              // ~N\r\n
    Push(Vec<RespValue>),                 // >N\r\n
    Resp3Null,                            // _\r\n
}

impl RespValue {
    pub fn ok() -> Self {
        RespValue::SimpleString("OK".to_string())
    }

    pub fn error(msg: impl Into<String>) -> Self {
        RespValue::Error(msg.into())
    }

    pub fn wrong_type() -> Self {
        RespValue::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string())
    }

    pub fn wrong_arity(cmd: &str) -> Self {
        RespValue::Error(format!("ERR wrong number of arguments for '{}' command", cmd))
    }

    pub fn integer(n: i64) -> Self {
        RespValue::Integer(n)
    }

    pub fn bulk_string(data: impl Into<Bytes>) -> Self {
        RespValue::BulkString(data.into())
    }

    pub fn simple_string(s: impl Into<String>) -> Self {
        RespValue::SimpleString(s.into())
    }

    pub fn array(items: Vec<RespValue>) -> Self {
        RespValue::Array(items)
    }

    /// Serialize this value into RESP wire format.
    pub fn serialize(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(64);
        self.write_to(&mut buf);
        buf.freeze()
    }

    pub fn write_to(&self, buf: &mut BytesMut) {
        match self {
            RespValue::SimpleString(s) => {
                buf.put_u8(b'+');
                buf.put_slice(s.as_bytes());
                buf.put_slice(b"\r\n");
            }
            RespValue::Error(s) => {
                buf.put_u8(b'-');
                buf.put_slice(s.as_bytes());
                buf.put_slice(b"\r\n");
            }
            RespValue::Integer(n) => {
                buf.put_u8(b':');
                buf.put_slice(n.to_string().as_bytes());
                buf.put_slice(b"\r\n");
            }
            RespValue::BulkString(data) => {
                buf.put_u8(b'$');
                buf.put_slice(data.len().to_string().as_bytes());
                buf.put_slice(b"\r\n");
                buf.put_slice(data);
                buf.put_slice(b"\r\n");
            }
            RespValue::Array(items) => {
                buf.put_u8(b'*');
                buf.put_slice(items.len().to_string().as_bytes());
                buf.put_slice(b"\r\n");
                for item in items {
                    item.write_to(buf);
                }
            }
            RespValue::Null => {
                buf.put_slice(b"$-1\r\n");
            }
            RespValue::NullArray => {
                buf.put_slice(b"*-1\r\n");
            }
            // RESP3 types
            RespValue::Double(d) => {
                buf.put_u8(b',');
                if d.is_infinite() {
                    if *d > 0.0 {
                        buf.put_slice(b"inf");
                    } else {
                        buf.put_slice(b"-inf");
                    }
                } else if d.is_nan() {
                    buf.put_slice(b"nan");
                } else {
                    buf.put_slice(d.to_string().as_bytes());
                }
                buf.put_slice(b"\r\n");
            }
            RespValue::Boolean(b_val) => {
                buf.put_u8(b'#');
                buf.put_u8(if *b_val { b't' } else { b'f' });
                buf.put_slice(b"\r\n");
            }
            RespValue::BlobError(data) => {
                buf.put_u8(b'!');
                buf.put_slice(data.len().to_string().as_bytes());
                buf.put_slice(b"\r\n");
                buf.put_slice(data);
                buf.put_slice(b"\r\n");
            }
            RespValue::VerbatimString(data) => {
                buf.put_u8(b'=');
                buf.put_slice(data.len().to_string().as_bytes());
                buf.put_slice(b"\r\n");
                buf.put_slice(data);
                buf.put_slice(b"\r\n");
            }
            RespValue::BigNumber(s) => {
                buf.put_u8(b'(');
                buf.put_slice(s.as_bytes());
                buf.put_slice(b"\r\n");
            }
            RespValue::Map(entries) => {
                buf.put_u8(b'%');
                buf.put_slice(entries.len().to_string().as_bytes());
                buf.put_slice(b"\r\n");
                for (key, val) in entries {
                    key.write_to(buf);
                    val.write_to(buf);
                }
            }
            RespValue::RespSet(items) => {
                buf.put_u8(b'~');
                buf.put_slice(items.len().to_string().as_bytes());
                buf.put_slice(b"\r\n");
                for item in items {
                    item.write_to(buf);
                }
            }
            RespValue::Push(items) => {
                buf.put_u8(b'>');
                buf.put_slice(items.len().to_string().as_bytes());
                buf.put_slice(b"\r\n");
                for item in items {
                    item.write_to(buf);
                }
            }
            RespValue::Resp3Null => {
                buf.put_slice(b"_\r\n");
            }
        }
    }

    /// Extract as a string (for command parsing).
    pub fn as_str(&self) -> Option<&[u8]> {
        match self {
            RespValue::BulkString(b) => Some(b),
            RespValue::SimpleString(s) => Some(s.as_bytes()),
            _ => None,
        }
    }

    pub fn to_string_lossy(&self) -> String {
        match self {
            RespValue::BulkString(b) => String::from_utf8_lossy(b).to_string(),
            RespValue::SimpleString(s) => s.clone(),
            RespValue::Integer(n) => n.to_string(),
            _ => String::new(),
        }
    }
}
