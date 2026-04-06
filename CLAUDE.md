# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

rCache is a Redis/Valkey-compatible in-memory data store written in Rust. Wire-compatible with Redis 7.2+ clients. MIT licensed, owned by Zeros And Ones LLC.

## Build Commands

```bash
cargo build          # Build
cargo check          # Type-check without building
cargo test           # Run all tests
cargo test <name>    # Run a single test by name
cargo clippy         # Lint
```

## Architecture

- **Single-threaded command execution** with async I/O via tokio (matches Redis model)
- Shared state via `Arc<Mutex<Store>>` — all commands acquire the store lock
- Per-connection async tasks handle read/parse/dispatch/respond cycle

### Module Layout

- `src/main.rs` — Entry point, config parsing, server launch
- `src/config.rs` — CLI arg parsing and Config struct
- `src/protocol/` — RESP2 parser (`parser.rs`) and value types (`types.rs`)
- `src/server/` — TCP listener (`mod.rs`) and per-connection handler (`connection.rs`)
- `src/storage/` — Store (multi-db), Database (single keyspace), RedisObject types, expiration
- `src/command/` — Command registry and handlers split by type:
  - `registry.rs` — Command dispatch (name -> handler mapping with arity checks)
  - `strings.rs`, `list.rs`, `set.rs`, `hash.rs`, `sorted_set.rs` — Data type commands
  - `keys.rs` — Key management (EXPIRE, TTL, RENAME, TYPE, SORT, etc.)
  - `scan.rs` — SCAN/SSCAN/HSCAN/ZSCAN
  - `server_cmds.rs` — PING, INFO, SELECT, FLUSHDB, etc.

### Key Patterns

- Command handlers are `fn(&mut CommandContext) -> RespValue`
- Always clone `ctx.args[N]` before calling `ctx.db()` (borrow checker constraint)
- Empty collections (list, set, hash, zset) are auto-deleted after mutations
- Sorted sets use `BTreeMap<ScoreKey, Bytes>` + `HashMap<Bytes, f64>` dual index

## Development Rules

- Increment `Cargo.toml` version before each commit (major=breaking, minor=features, patch=fixes)
- Run `cargo check` before committing; fix all errors and warnings properly
- Use async when possible
- No paid crates without explicit approval
- See `plan.md` for the phased implementation roadmap
