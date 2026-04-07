# Security Policy

## Reporting a Vulnerability

If you discover a security vulnerability in rCache, please report it responsibly.

**Do not open a public GitHub issue for security vulnerabilities.**

Email: support@zerosandones.us

Include:
- Description of the vulnerability
- Steps to reproduce
- Potential impact
- Suggested fix (if any)

We will acknowledge receipt within 48 hours and provide a timeline for a fix.

## Supported Versions

| Version | Supported |
|---------|-----------|
| 0.10.x  | Yes       |
| < 0.10  | No        |

## Security Considerations

### Authentication
- Use `--requirepass` to require authentication
- Use the ACL system for per-user permissions (`ACL SETUSER`)
- Use TLS (`--tls-port`) for encrypted connections

### Network
- Bind to specific interfaces (`--bind 127.0.0.1`) instead of `0.0.0.0` in production
- Use TLS for all client connections in production
- Use firewall rules to restrict access to the rCache port

### Commands
- Disable dangerous commands via ACL (`-@dangerous`)
- `KEYS *` is O(n) and should not be used in production (use `SCAN` instead)
- `FLUSHALL` / `FLUSHDB` can be restricted via ACL
- `DEBUG` and `CONFIG` should be restricted in production

### Persistence
- RDB and AOF files may contain sensitive data — restrict file permissions
- RDB files include CRC64 checksums for integrity verification
