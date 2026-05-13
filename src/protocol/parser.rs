use bytes::{Bytes, BytesMut};
use super::types::RespValue;

/// Streaming RESP2/RESP3 protocol parser.
pub struct Parser;

#[derive(Debug)]
pub enum ParseError {
    Incomplete,
    Invalid(String),
}

/// Maximum size of a single bulk string (matches real Redis: 512 MB).
const MAX_BULK_LEN: usize = 512 * 1024 * 1024;
/// Maximum number of elements in any aggregate (array, map, set, push).
const MAX_MULTIBULK_LEN: usize = 1_048_576;
/// Maximum nesting depth for aggregates — guards against stack overflow.
const MAX_DEPTH: u32 = 32;

impl Parser {
    /// Try to parse one complete RESP value from the buffer.
    /// On success, returns the value and number of bytes consumed.
    /// On Incomplete, the caller should read more data and retry.
    pub fn parse(buf: &BytesMut) -> Result<(RespValue, usize), ParseError> {
        Self::parse_at(&buf[..], 0)
    }

    fn parse_at(buf: &[u8], depth: u32) -> Result<(RespValue, usize), ParseError> {
        if depth > MAX_DEPTH {
            return Err(ParseError::Invalid("Protocol nesting depth exceeded".to_string()));
        }
        if buf.is_empty() {
            return Err(ParseError::Incomplete);
        }

        match buf[0] {
            b'+' => Self::parse_simple_string(buf),
            b'-' => Self::parse_error(buf),
            b':' => Self::parse_integer(buf),
            b'$' => Self::parse_bulk_string(buf),
            b'*' => Self::parse_array(buf, depth),
            // RESP3 types
            b',' => Self::parse_double(buf),
            b'#' => Self::parse_boolean(buf),
            b'!' => Self::parse_blob_error(buf),
            b'=' => Self::parse_verbatim_string(buf),
            b'(' => Self::parse_big_number(buf),
            b'%' => Self::parse_map(buf, depth),
            b'~' => Self::parse_set(buf, depth),
            b'>' => Self::parse_push(buf, depth),
            b'_' => Self::parse_resp3_null(buf),
            _ => Self::parse_inline(buf),
        }
    }

    fn parse_simple_string(buf: &[u8]) -> Result<(RespValue, usize), ParseError> {
        let (line, consumed) = Self::read_line(buf, 1)?;
        let s = String::from_utf8_lossy(line).to_string();
        Ok((RespValue::SimpleString(s), consumed))
    }

    fn parse_error(buf: &[u8]) -> Result<(RespValue, usize), ParseError> {
        let (line, consumed) = Self::read_line(buf, 1)?;
        let s = String::from_utf8_lossy(line).to_string();
        Ok((RespValue::Error(s), consumed))
    }

    fn parse_integer(buf: &[u8]) -> Result<(RespValue, usize), ParseError> {
        let (line, consumed) = Self::read_line(buf, 1)?;
        let s = std::str::from_utf8(line)
            .map_err(|e| ParseError::Invalid(e.to_string()))?;
        let n: i64 = s.parse()
            .map_err(|e: std::num::ParseIntError| ParseError::Invalid(e.to_string()))?;
        Ok((RespValue::Integer(n), consumed))
    }

    fn parse_bulk_string(buf: &[u8]) -> Result<(RespValue, usize), ParseError> {
        let (line, header_consumed) = Self::read_line(buf, 1)?;
        let len_str = std::str::from_utf8(line)
            .map_err(|e| ParseError::Invalid(e.to_string()))?;
        let len: i64 = len_str.parse()
            .map_err(|e: std::num::ParseIntError| ParseError::Invalid(e.to_string()))?;

        if len == -1 {
            return Ok((RespValue::Null, header_consumed));
        }

        if len < 0 {
            return Err(ParseError::Invalid("invalid bulk string length".to_string()));
        }

        let len = len as usize;
        if len > MAX_BULK_LEN {
            return Err(ParseError::Invalid("bulk string too long".to_string()));
        }

        let total_needed = header_consumed.checked_add(len)
            .and_then(|x| x.checked_add(2))
            .ok_or_else(|| ParseError::Invalid("length overflow".to_string()))?;

        if buf.len() < total_needed {
            return Err(ParseError::Incomplete);
        }

        let data_slice = buf.get(header_consumed..header_consumed + len)
            .ok_or(ParseError::Incomplete)?;
        Ok((RespValue::BulkString(Bytes::copy_from_slice(data_slice)), total_needed))
    }

    fn parse_array(buf: &[u8], depth: u32) -> Result<(RespValue, usize), ParseError> {
        let (line, mut consumed) = Self::read_line(buf, 1)?;
        let len_str = std::str::from_utf8(line)
            .map_err(|e| ParseError::Invalid(e.to_string()))?;
        let len: i64 = len_str.parse()
            .map_err(|e: std::num::ParseIntError| ParseError::Invalid(e.to_string()))?;

        if len == -1 {
            return Ok((RespValue::NullArray, consumed));
        }

        if len < 0 {
            return Err(ParseError::Invalid("invalid array length".to_string()));
        }

        let len = len as usize;
        if len > MAX_MULTIBULK_LEN {
            return Err(ParseError::Invalid("multibulk length out of range".to_string()));
        }

        let mut items = Vec::with_capacity(len);

        for _ in 0..len {
            let rest = buf.get(consumed..).ok_or(ParseError::Incomplete)?;
            let (value, n) = Self::parse_at(rest, depth + 1)?;
            items.push(value);
            consumed += n;
        }

        Ok((RespValue::Array(items), consumed))
    }

    // === RESP3 parsers ===

    fn parse_double(buf: &[u8]) -> Result<(RespValue, usize), ParseError> {
        let (line, consumed) = Self::read_line(buf, 1)?;
        let s = std::str::from_utf8(line)
            .map_err(|e| ParseError::Invalid(e.to_string()))?;
        let d: f64 = match s {
            "inf" => f64::INFINITY,
            "-inf" => f64::NEG_INFINITY,
            "nan" => f64::NAN,
            _ => s.parse().map_err(|e: std::num::ParseFloatError| ParseError::Invalid(e.to_string()))?,
        };
        Ok((RespValue::Double(d), consumed))
    }

    fn parse_boolean(buf: &[u8]) -> Result<(RespValue, usize), ParseError> {
        let (line, consumed) = Self::read_line(buf, 1)?;
        match line {
            b"t" => Ok((RespValue::Boolean(true), consumed)),
            b"f" => Ok((RespValue::Boolean(false), consumed)),
            _ => Err(ParseError::Invalid("invalid boolean value".to_string())),
        }
    }

    fn parse_blob_error(buf: &[u8]) -> Result<(RespValue, usize), ParseError> {
        let (line, header_consumed) = Self::read_line(buf, 1)?;
        let len_str = std::str::from_utf8(line)
            .map_err(|e| ParseError::Invalid(e.to_string()))?;
        let len: usize = len_str.parse()
            .map_err(|e: std::num::ParseIntError| ParseError::Invalid(e.to_string()))?;

        if len > MAX_BULK_LEN {
            return Err(ParseError::Invalid("blob error too long".to_string()));
        }

        let total_needed = header_consumed.checked_add(len)
            .and_then(|x| x.checked_add(2))
            .ok_or_else(|| ParseError::Invalid("length overflow".to_string()))?;
        if buf.len() < total_needed {
            return Err(ParseError::Incomplete);
        }

        let data_slice = buf.get(header_consumed..header_consumed + len)
            .ok_or(ParseError::Incomplete)?;
        Ok((RespValue::BlobError(Bytes::copy_from_slice(data_slice)), total_needed))
    }

    fn parse_verbatim_string(buf: &[u8]) -> Result<(RespValue, usize), ParseError> {
        let (line, header_consumed) = Self::read_line(buf, 1)?;
        let len_str = std::str::from_utf8(line)
            .map_err(|e| ParseError::Invalid(e.to_string()))?;
        let len: usize = len_str.parse()
            .map_err(|e: std::num::ParseIntError| ParseError::Invalid(e.to_string()))?;

        if len > MAX_BULK_LEN {
            return Err(ParseError::Invalid("verbatim string too long".to_string()));
        }

        let total_needed = header_consumed.checked_add(len)
            .and_then(|x| x.checked_add(2))
            .ok_or_else(|| ParseError::Invalid("length overflow".to_string()))?;
        if buf.len() < total_needed {
            return Err(ParseError::Incomplete);
        }

        let data_slice = buf.get(header_consumed..header_consumed + len)
            .ok_or(ParseError::Incomplete)?;
        Ok((RespValue::VerbatimString(Bytes::copy_from_slice(data_slice)), total_needed))
    }

    fn parse_big_number(buf: &[u8]) -> Result<(RespValue, usize), ParseError> {
        let (line, consumed) = Self::read_line(buf, 1)?;
        let s = String::from_utf8_lossy(line).to_string();
        Ok((RespValue::BigNumber(s), consumed))
    }

    fn parse_map(buf: &[u8], depth: u32) -> Result<(RespValue, usize), ParseError> {
        let (line, mut consumed) = Self::read_line(buf, 1)?;
        let len_str = std::str::from_utf8(line)
            .map_err(|e| ParseError::Invalid(e.to_string()))?;
        let len: i64 = len_str.parse()
            .map_err(|e: std::num::ParseIntError| ParseError::Invalid(e.to_string()))?;

        if len < 0 {
            return Err(ParseError::Invalid("invalid map length".to_string()));
        }

        let len = len as usize;
        if len > MAX_MULTIBULK_LEN {
            return Err(ParseError::Invalid("map length out of range".to_string()));
        }
        let mut entries = Vec::with_capacity(len);

        for _ in 0..len {
            let rest = buf.get(consumed..).ok_or(ParseError::Incomplete)?;
            let (key, n) = Self::parse_at(rest, depth + 1)?;
            consumed += n;

            let rest = buf.get(consumed..).ok_or(ParseError::Incomplete)?;
            let (val, n) = Self::parse_at(rest, depth + 1)?;
            consumed += n;

            entries.push((key, val));
        }

        Ok((RespValue::Map(entries), consumed))
    }

    fn parse_set(buf: &[u8], depth: u32) -> Result<(RespValue, usize), ParseError> {
        let (line, mut consumed) = Self::read_line(buf, 1)?;
        let len_str = std::str::from_utf8(line)
            .map_err(|e| ParseError::Invalid(e.to_string()))?;
        let len: i64 = len_str.parse()
            .map_err(|e: std::num::ParseIntError| ParseError::Invalid(e.to_string()))?;

        if len < 0 {
            return Err(ParseError::Invalid("invalid set length".to_string()));
        }

        let len = len as usize;
        if len > MAX_MULTIBULK_LEN {
            return Err(ParseError::Invalid("set length out of range".to_string()));
        }
        let mut items = Vec::with_capacity(len);

        for _ in 0..len {
            let rest = buf.get(consumed..).ok_or(ParseError::Incomplete)?;
            let (value, n) = Self::parse_at(rest, depth + 1)?;
            items.push(value);
            consumed += n;
        }

        Ok((RespValue::RespSet(items), consumed))
    }

    fn parse_push(buf: &[u8], depth: u32) -> Result<(RespValue, usize), ParseError> {
        let (line, mut consumed) = Self::read_line(buf, 1)?;
        let len_str = std::str::from_utf8(line)
            .map_err(|e| ParseError::Invalid(e.to_string()))?;
        let len: i64 = len_str.parse()
            .map_err(|e: std::num::ParseIntError| ParseError::Invalid(e.to_string()))?;

        if len < 0 {
            return Err(ParseError::Invalid("invalid push length".to_string()));
        }

        let len = len as usize;
        if len > MAX_MULTIBULK_LEN {
            return Err(ParseError::Invalid("push length out of range".to_string()));
        }
        let mut items = Vec::with_capacity(len);

        for _ in 0..len {
            let rest = buf.get(consumed..).ok_or(ParseError::Incomplete)?;
            let (value, n) = Self::parse_at(rest, depth + 1)?;
            items.push(value);
            consumed += n;
        }

        Ok((RespValue::Push(items), consumed))
    }

    fn parse_resp3_null(buf: &[u8]) -> Result<(RespValue, usize), ParseError> {
        let (_line, consumed) = Self::read_line(buf, 1)?;
        Ok((RespValue::Resp3Null, consumed))
    }

    /// Parse an inline command (space-delimited, no RESP prefix).
    fn parse_inline(buf: &[u8]) -> Result<(RespValue, usize), ParseError> {
        let (line, consumed) = Self::read_line(buf, 0)?;
        if line.is_empty() {
            return Err(ParseError::Incomplete);
        }

        let line_str = std::str::from_utf8(line)
            .map_err(|e| ParseError::Invalid(e.to_string()))?;
        let parts: Vec<RespValue> = line_str
            .split_whitespace()
            .map(|s| RespValue::BulkString(Bytes::copy_from_slice(s.as_bytes())))
            .collect();

        if parts.is_empty() {
            return Err(ParseError::Incomplete);
        }

        Ok((RespValue::Array(parts), consumed))
    }

    /// Read a line from the buffer starting at `offset`.
    /// Returns the line content (without \r\n) and total bytes consumed from start of buffer.
    fn read_line(buf: &[u8], offset: usize) -> Result<(&[u8], usize), ParseError> {
        if offset >= buf.len() {
            return Err(ParseError::Incomplete);
        }

        for i in offset..buf.len().saturating_sub(1) {
            if buf[i] == b'\r' && buf[i + 1] == b'\n' {
                return Ok((&buf[offset..i], i + 2));
            }
        }

        Err(ParseError::Incomplete)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_string() {
        let buf = BytesMut::from("+OK\r\n");
        let (val, consumed) = Parser::parse(&buf).unwrap();
        assert_eq!(val, RespValue::SimpleString("OK".to_string()));
        assert_eq!(consumed, 5);
    }

    #[test]
    fn test_error() {
        let buf = BytesMut::from("-ERR unknown\r\n");
        let (val, consumed) = Parser::parse(&buf).unwrap();
        assert_eq!(val, RespValue::Error("ERR unknown".to_string()));
        assert_eq!(consumed, 14);
    }

    #[test]
    fn test_integer() {
        let buf = BytesMut::from(":1000\r\n");
        let (val, _) = Parser::parse(&buf).unwrap();
        assert_eq!(val, RespValue::Integer(1000));
    }

    #[test]
    fn test_negative_integer() {
        let buf = BytesMut::from(":-42\r\n");
        let (val, _) = Parser::parse(&buf).unwrap();
        assert_eq!(val, RespValue::Integer(-42));
    }

    #[test]
    fn test_bulk_string() {
        let buf = BytesMut::from("$5\r\nHello\r\n");
        let (val, consumed) = Parser::parse(&buf).unwrap();
        assert_eq!(val, RespValue::BulkString("Hello".as_bytes().to_vec().into()));
        assert_eq!(consumed, 11);
    }

    #[test]
    fn test_empty_bulk_string() {
        let buf = BytesMut::from("$0\r\n\r\n");
        let (val, consumed) = Parser::parse(&buf).unwrap();
        assert_eq!(val, RespValue::BulkString(bytes::Bytes::new()));
        assert_eq!(consumed, 6);
    }

    #[test]
    fn test_null_bulk_string() {
        let buf = BytesMut::from("$-1\r\n");
        let (val, consumed) = Parser::parse(&buf).unwrap();
        assert_eq!(val, RespValue::Null);
        assert_eq!(consumed, 5);
    }

    #[test]
    fn test_array() {
        let buf = BytesMut::from("*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n");
        let (val, consumed) = Parser::parse(&buf).unwrap();
        match val {
            RespValue::Array(items) => {
                assert_eq!(items.len(), 2);
                assert_eq!(items[0], RespValue::BulkString("GET".as_bytes().to_vec().into()));
                assert_eq!(items[1], RespValue::BulkString("key".as_bytes().to_vec().into()));
            }
            _ => panic!("expected array"),
        }
        assert_eq!(consumed, 22);
    }

    #[test]
    fn test_null_array() {
        let buf = BytesMut::from("*-1\r\n");
        let (val, _) = Parser::parse(&buf).unwrap();
        assert_eq!(val, RespValue::NullArray);
    }

    #[test]
    fn test_empty_array() {
        let buf = BytesMut::from("*0\r\n");
        let (val, _) = Parser::parse(&buf).unwrap();
        assert_eq!(val, RespValue::Array(vec![]));
    }

    #[test]
    fn test_nested_array() {
        let buf = BytesMut::from("*2\r\n*1\r\n:1\r\n*1\r\n:2\r\n");
        let (val, _) = Parser::parse(&buf).unwrap();
        match val {
            RespValue::Array(items) => {
                assert_eq!(items.len(), 2);
                assert_eq!(items[0], RespValue::Array(vec![RespValue::Integer(1)]));
                assert_eq!(items[1], RespValue::Array(vec![RespValue::Integer(2)]));
            }
            _ => panic!("expected array"),
        }
    }

    #[test]
    fn test_inline_command() {
        let buf = BytesMut::from("PING\r\n");
        let (val, _) = Parser::parse(&buf).unwrap();
        match val {
            RespValue::Array(items) => {
                assert_eq!(items.len(), 1);
                assert_eq!(items[0], RespValue::BulkString("PING".as_bytes().to_vec().into()));
            }
            _ => panic!("expected array"),
        }
    }

    #[test]
    fn test_inline_with_args() {
        let buf = BytesMut::from("SET key value\r\n");
        let (val, _) = Parser::parse(&buf).unwrap();
        match val {
            RespValue::Array(items) => {
                assert_eq!(items.len(), 3);
            }
            _ => panic!("expected array"),
        }
    }

    #[test]
    fn test_incomplete_data() {
        let buf = BytesMut::from("$5\r\nHel");
        assert!(matches!(Parser::parse(&buf), Err(ParseError::Incomplete)));
    }

    #[test]
    fn test_rejects_oversize_bulk() {
        // $<huge>\r\n — must be rejected before allocating
        let buf = BytesMut::from("$2147483647\r\n");
        match Parser::parse(&buf) {
            Err(ParseError::Invalid(_)) => {}
            other => panic!("expected Invalid, got {:?}", other),
        }
    }

    #[test]
    fn test_rejects_oversize_multibulk() {
        let buf = BytesMut::from("*9999999\r\n");
        match Parser::parse(&buf) {
            Err(ParseError::Invalid(_)) => {}
            other => panic!("expected Invalid, got {:?}", other),
        }
    }

    #[test]
    fn test_rejects_deep_nesting() {
        // Build a string of N nested *1\r\n followed by :1\r\n
        let mut s = String::new();
        for _ in 0..(MAX_DEPTH + 5) {
            s.push_str("*1\r\n");
        }
        s.push_str(":1\r\n");
        let buf = BytesMut::from(s.as_str());
        match Parser::parse(&buf) {
            Err(ParseError::Invalid(_)) => {}
            other => panic!("expected Invalid, got {:?}", other),
        }
    }

    #[test]
    fn test_serialization_roundtrip() {
        let values = vec![
            RespValue::SimpleString("OK".to_string()),
            RespValue::Error("ERR test".to_string()),
            RespValue::Integer(42),
            RespValue::BulkString("hello".as_bytes().to_vec().into()),
            RespValue::Null,
            RespValue::NullArray,
            RespValue::Array(vec![
                RespValue::Integer(1),
                RespValue::BulkString("two".as_bytes().to_vec().into()),
            ]),
        ];

        for val in values {
            let serialized = val.serialize();
            let buf = BytesMut::from(serialized.as_ref());
            let (parsed, _) = Parser::parse(&buf).unwrap();
            assert_eq!(val, parsed);
        }
    }

    // === RESP3 tests ===

    #[test]
    fn test_resp3_double() {
        let buf = BytesMut::from(",3.14\r\n");
        let (val, consumed) = Parser::parse(&buf).unwrap();
        assert_eq!(val, RespValue::Double(3.14));
        assert_eq!(consumed, 7);
    }

    #[test]
    fn test_resp3_double_inf() {
        let buf = BytesMut::from(",inf\r\n");
        let (val, _) = Parser::parse(&buf).unwrap();
        assert_eq!(val, RespValue::Double(f64::INFINITY));

        let buf = BytesMut::from(",-inf\r\n");
        let (val, _) = Parser::parse(&buf).unwrap();
        assert_eq!(val, RespValue::Double(f64::NEG_INFINITY));
    }

    #[test]
    fn test_resp3_boolean() {
        let buf = BytesMut::from("#t\r\n");
        let (val, consumed) = Parser::parse(&buf).unwrap();
        assert_eq!(val, RespValue::Boolean(true));
        assert_eq!(consumed, 4);

        let buf = BytesMut::from("#f\r\n");
        let (val, consumed) = Parser::parse(&buf).unwrap();
        assert_eq!(val, RespValue::Boolean(false));
        assert_eq!(consumed, 4);
    }

    #[test]
    fn test_resp3_blob_error() {
        let buf = BytesMut::from("!11\r\nERR unknown\r\n");
        let (val, consumed) = Parser::parse(&buf).unwrap();
        assert_eq!(val, RespValue::BlobError(bytes::Bytes::from("ERR unknown")));
        assert_eq!(consumed, 18);
    }

    #[test]
    fn test_resp3_verbatim_string() {
        let buf = BytesMut::from("=15\r\ntxt:Hello World\r\n");
        let (val, consumed) = Parser::parse(&buf).unwrap();
        assert_eq!(val, RespValue::VerbatimString(bytes::Bytes::from("txt:Hello World")));
        assert_eq!(consumed, 22);
    }

    #[test]
    fn test_resp3_big_number() {
        let buf = BytesMut::from("(12345678901234567890\r\n");
        let (val, _) = Parser::parse(&buf).unwrap();
        assert_eq!(val, RespValue::BigNumber("12345678901234567890".to_string()));
    }

    #[test]
    fn test_resp3_null() {
        let buf = BytesMut::from("_\r\n");
        let (val, consumed) = Parser::parse(&buf).unwrap();
        assert_eq!(val, RespValue::Resp3Null);
        assert_eq!(consumed, 3);
    }

    #[test]
    fn test_resp3_map() {
        let buf = BytesMut::from("%2\r\n+key1\r\n:1\r\n+key2\r\n:2\r\n");
        let (val, _) = Parser::parse(&buf).unwrap();
        match val {
            RespValue::Map(entries) => {
                assert_eq!(entries.len(), 2);
                assert_eq!(entries[0].0, RespValue::SimpleString("key1".to_string()));
                assert_eq!(entries[0].1, RespValue::Integer(1));
                assert_eq!(entries[1].0, RespValue::SimpleString("key2".to_string()));
                assert_eq!(entries[1].1, RespValue::Integer(2));
            }
            _ => panic!("expected map"),
        }
    }

    #[test]
    fn test_resp3_set() {
        let buf = BytesMut::from("~2\r\n:1\r\n:2\r\n");
        let (val, _) = Parser::parse(&buf).unwrap();
        match val {
            RespValue::RespSet(items) => {
                assert_eq!(items.len(), 2);
                assert_eq!(items[0], RespValue::Integer(1));
                assert_eq!(items[1], RespValue::Integer(2));
            }
            _ => panic!("expected set"),
        }
    }

    #[test]
    fn test_resp3_push() {
        let buf = BytesMut::from(">2\r\n+message\r\n$5\r\nhello\r\n");
        let (val, _) = Parser::parse(&buf).unwrap();
        match val {
            RespValue::Push(items) => {
                assert_eq!(items.len(), 2);
                assert_eq!(items[0], RespValue::SimpleString("message".to_string()));
                assert_eq!(items[1], RespValue::BulkString(bytes::Bytes::from("hello")));
            }
            _ => panic!("expected push"),
        }
    }

    #[test]
    fn test_resp3_serialization_roundtrip() {
        let values = vec![
            RespValue::Double(3.14),
            RespValue::Double(f64::INFINITY),
            RespValue::Double(f64::NEG_INFINITY),
            RespValue::Boolean(true),
            RespValue::Boolean(false),
            RespValue::BlobError(bytes::Bytes::from("ERR test")),
            RespValue::VerbatimString(bytes::Bytes::from("txt:hello")),
            RespValue::BigNumber("12345".to_string()),
            RespValue::Resp3Null,
            RespValue::Map(vec![
                (RespValue::SimpleString("a".to_string()), RespValue::Integer(1)),
            ]),
            RespValue::RespSet(vec![RespValue::Integer(1), RespValue::Integer(2)]),
            RespValue::Push(vec![RespValue::SimpleString("msg".to_string())]),
        ];

        for val in values {
            let serialized = val.serialize();
            let buf = BytesMut::from(serialized.as_ref());
            let (parsed, _) = Parser::parse(&buf).unwrap();
            assert_eq!(val, parsed, "roundtrip failed for {:?}", val);
        }
    }
}
