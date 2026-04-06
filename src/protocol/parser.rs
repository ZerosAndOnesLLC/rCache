use bytes::BytesMut;
use super::types::RespValue;

/// Streaming RESP2 protocol parser.
pub struct Parser;

#[derive(Debug)]
pub enum ParseError {
    Incomplete,
    Invalid(String),
}

impl Parser {
    /// Try to parse one complete RESP value from the buffer.
    /// On success, returns the value and number of bytes consumed.
    /// On Incomplete, the caller should read more data and retry.
    pub fn parse(buf: &BytesMut) -> Result<(RespValue, usize), ParseError> {
        if buf.is_empty() {
            return Err(ParseError::Incomplete);
        }

        match buf[0] {
            b'+' => Self::parse_simple_string(buf),
            b'-' => Self::parse_error(buf),
            b':' => Self::parse_integer(buf),
            b'$' => Self::parse_bulk_string(buf),
            b'*' => Self::parse_array(buf),
            _ => Self::parse_inline(buf),
        }
    }

    fn parse_simple_string(buf: &BytesMut) -> Result<(RespValue, usize), ParseError> {
        let (line, consumed) = Self::read_line(buf, 1)?;
        let s = String::from_utf8_lossy(line).to_string();
        Ok((RespValue::SimpleString(s), consumed))
    }

    fn parse_error(buf: &BytesMut) -> Result<(RespValue, usize), ParseError> {
        let (line, consumed) = Self::read_line(buf, 1)?;
        let s = String::from_utf8_lossy(line).to_string();
        Ok((RespValue::Error(s), consumed))
    }

    fn parse_integer(buf: &BytesMut) -> Result<(RespValue, usize), ParseError> {
        let (line, consumed) = Self::read_line(buf, 1)?;
        let s = std::str::from_utf8(line)
            .map_err(|e| ParseError::Invalid(e.to_string()))?;
        let n: i64 = s.parse()
            .map_err(|e: std::num::ParseIntError| ParseError::Invalid(e.to_string()))?;
        Ok((RespValue::Integer(n), consumed))
    }

    fn parse_bulk_string(buf: &BytesMut) -> Result<(RespValue, usize), ParseError> {
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
        let total_needed = header_consumed + len + 2; // data + \r\n

        if buf.len() < total_needed {
            return Err(ParseError::Incomplete);
        }

        let data = buf[header_consumed..header_consumed + len].to_vec();
        Ok((RespValue::BulkString(data.into()), total_needed))
    }

    fn parse_array(buf: &BytesMut) -> Result<(RespValue, usize), ParseError> {
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
        let mut items = Vec::with_capacity(len);

        for _ in 0..len {
            let remaining = BytesMut::from(&buf[consumed..]);
            let (value, n) = Self::parse(&remaining)?;
            items.push(value);
            consumed += n;
        }

        Ok((RespValue::Array(items), consumed))
    }

    /// Parse an inline command (space-delimited, no RESP prefix).
    fn parse_inline(buf: &BytesMut) -> Result<(RespValue, usize), ParseError> {
        let (line, consumed) = Self::read_line(buf, 0)?;
        if line.is_empty() {
            return Err(ParseError::Incomplete);
        }

        let line_str = String::from_utf8_lossy(line);
        let parts: Vec<RespValue> = line_str
            .split_whitespace()
            .map(|s| RespValue::BulkString(s.as_bytes().to_vec().into()))
            .collect();

        if parts.is_empty() {
            return Err(ParseError::Incomplete);
        }

        Ok((RespValue::Array(parts), consumed))
    }

    /// Read a line from the buffer starting at `offset`.
    /// Returns the line content (without \r\n) and total bytes consumed from start of buffer.
    fn read_line(buf: &BytesMut, offset: usize) -> Result<(&[u8], usize), ParseError> {
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
}
