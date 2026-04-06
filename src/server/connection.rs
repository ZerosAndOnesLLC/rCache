use std::sync::Arc;
use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::protocol::{Parser, RespValue};
use crate::command::CommandContext;
use super::SharedState;

pub struct Connection {
    stream: TcpStream,
    state: Arc<SharedState>,
    db_index: usize,
    authenticated: bool,
    buffer: BytesMut,
}

impl Connection {
    pub fn new(stream: TcpStream, state: Arc<SharedState>) -> Self {
        let authenticated = state.config.requirepass.is_none();
        Self {
            stream,
            state,
            db_index: 0,
            authenticated,
            buffer: BytesMut::with_capacity(4096),
        }
    }

    pub async fn handle(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        loop {
            // Try to parse a complete command from the buffer
            while let Some(response) = self.try_process_command().await? {
                let data = response.serialize();
                self.stream.write_all(&data).await?;
            }

            // Read more data from the socket
            let n = self.stream.read_buf(&mut self.buffer).await?;
            if n == 0 {
                // Connection closed
                return Ok(());
            }
        }
    }

    async fn try_process_command(&mut self) -> Result<Option<RespValue>, Box<dyn std::error::Error + Send + Sync>> {
        if self.buffer.is_empty() {
            return Ok(None);
        }

        match Parser::parse(&self.buffer) {
            Ok((value, consumed)) => {
                let _ = self.buffer.split_to(consumed);
                let response = self.execute_command(value).await;
                Ok(Some(response))
            }
            Err(crate::protocol::parser::ParseError::Incomplete) => Ok(None),
            Err(crate::protocol::parser::ParseError::Invalid(msg)) => {
                Ok(Some(RespValue::error(format!("ERR protocol error: {}", msg))))
            }
        }
    }

    async fn execute_command(&mut self, value: RespValue) -> RespValue {
        let args = match value {
            RespValue::Array(items) => {
                items.into_iter().map(|v| match v {
                    RespValue::BulkString(b) => b,
                    other => bytes::Bytes::from(other.to_string_lossy()),
                }).collect::<Vec<_>>()
            }
            _ => return RespValue::error("ERR invalid command format"),
        };

        if args.is_empty() {
            return RespValue::error("ERR empty command");
        }

        let cmd_name = String::from_utf8_lossy(&args[0]).to_uppercase();

        // Handle QUIT
        if cmd_name == "QUIT" {
            return RespValue::ok();
        }

        // Handle AUTH
        if cmd_name == "AUTH" {
            return self.handle_auth(&args);
        }

        // Check authentication
        if !self.authenticated {
            return RespValue::error("NOAUTH Authentication required.");
        }

        // Handle SELECT specially (modifies connection state)
        if cmd_name == "SELECT" {
            if args.len() != 2 {
                return RespValue::wrong_arity("select");
            }
            let index: usize = match String::from_utf8_lossy(&args[1]).parse() {
                Ok(v) => v,
                Err(_) => return RespValue::error("ERR value is not an integer or out of range"),
            };
            let store = self.state.store.lock().await;
            if index >= store.db_count() {
                return RespValue::error("ERR DB index is out of range");
            }
            drop(store);
            self.db_index = index;
            return RespValue::ok();
        }

        // Execute command with store lock
        let mut store = self.state.store.lock().await;
        let mut ctx = CommandContext {
            store: &mut store,
            db_index: self.db_index,
            args,
            start_time: self.state.start_time,
        };

        let result = self.state.registry.execute(&mut ctx);

        // Update db_index in case SELECT was called via some path
        self.db_index = ctx.db_index;

        result
    }

    fn handle_auth(&mut self, args: &[bytes::Bytes]) -> RespValue {
        if args.len() < 2 {
            return RespValue::wrong_arity("auth");
        }

        match &self.state.config.requirepass {
            Some(password) => {
                let provided = if args.len() >= 3 {
                    // AUTH username password (Redis 6+ ACL style, ignore username for now)
                    String::from_utf8_lossy(&args[2]).to_string()
                } else {
                    String::from_utf8_lossy(&args[1]).to_string()
                };

                if provided == *password {
                    self.authenticated = true;
                    RespValue::ok()
                } else {
                    RespValue::error("WRONGPASS invalid username-password pair or user is disabled.")
                }
            }
            None => {
                RespValue::error("ERR Client sent AUTH, but no password is set. Did you mean ACL SETUSER with >password?")
            }
        }
    }
}
