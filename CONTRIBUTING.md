# Contributing to rCache

## Getting Started

```bash
git clone git@github.com:ZerosAndOnesLLC/rCache.git
cd rCache
cargo build
cargo test
```

## Development Workflow

1. Create a branch from `main`
2. Make your changes
3. Run `cargo check` — fix all errors and warnings
4. Run `cargo test` — all tests must pass
5. Bump the version in `Cargo.toml` (patch for fixes, minor for features, major for breaking changes)
6. Submit a pull request

## Code Style

- Follow existing patterns in the codebase
- Command handlers are `fn(&mut CommandContext) -> RespValue`
- Always clone `ctx.args[N]` before calling `ctx.db()` (borrow checker requirement)
- Clean up empty collections after element removal
- Return `WRONGTYPE` for type mismatches
- Match Redis error message formats exactly

## Adding a New Command

1. Add the handler function in the appropriate file under `src/command/`
2. Register it in `src/command/registry.rs` with correct arity
3. If it's a write command, add it to `is_write_command()` in `src/persistence/aof.rs`
4. Handle the new command in keyspace notifications if applicable
5. Add tests

## Architecture

- `src/protocol/` — RESP2/RESP3 parser and serializer
- `src/server/` — TCP/TLS listener, connection handler, pub/sub, blocking ops
- `src/storage/` — Multi-database store, key-value storage, expiration, eviction
- `src/command/` — All command handlers (one file per category)
- `src/persistence/` — RDB snapshots and AOF logging
- `src/http.rs` — HTTP REST API and Prometheus metrics

## Testing

```bash
cargo test                    # Run all tests
cargo test test_name          # Run a specific test
cargo test --release          # Run tests in release mode
redis-benchmark -p 6379       # Performance benchmarking
```

## Reporting Issues

Open an issue at https://github.com/ZerosAndOnesLLC/rCache/issues with:
- What you expected to happen
- What actually happened
- Steps to reproduce
- Redis/Valkey command that behaves differently (if applicable)
