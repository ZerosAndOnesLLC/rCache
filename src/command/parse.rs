//! Lightweight argument parsers shared across command handlers.
//!
//! Replaces the common `String::from_utf8_lossy(&bytes).parse()` chain — which
//! allocates a Cow + an owned String per call and silently accepts malformed
//! UTF-8 — with strict `std::str::from_utf8` followed by `.parse()`. Errors are
//! surfaced as `None` so callers can attach a command-specific RESP error.

use bytes::Bytes;

#[inline]
pub fn int(b: &Bytes) -> Option<i64> {
    std::str::from_utf8(b).ok()?.parse().ok()
}

#[inline]
pub fn u64_(b: &Bytes) -> Option<u64> {
    std::str::from_utf8(b).ok()?.parse().ok()
}

#[inline]
pub fn usize_(b: &Bytes) -> Option<usize> {
    std::str::from_utf8(b).ok()?.parse().ok()
}

#[inline]
pub fn float(b: &Bytes) -> Option<f64> {
    std::str::from_utf8(b).ok()?.parse().ok()
}
