use bytes::{Bytes, BytesMut, BufMut};

/// RESP protocol value types.
#[derive(Debug, Clone, PartialEq)]
pub enum RespValue {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Bytes),
    Array(Vec<RespValue>),
    Null,
    NullArray,
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
