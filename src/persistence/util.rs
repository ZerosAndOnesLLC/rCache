use std::fs::{File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};

/// Restrictive permission bits for persistence files (owner read/write only).
/// The RDB/AOF hold the full dataset in cleartext, so they must not be
/// world-readable on a multi-user host.
#[cfg(unix)]
pub const PERSIST_FILE_MODE: u32 = 0o600;

/// Apply the restrictive persistence-file mode to an `OpenOptions` (no-op on
/// non-Unix targets).
pub fn with_secure_mode(opts: &mut OpenOptions) -> &mut OpenOptions {
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(PERSIST_FILE_MODE);
    }
    opts
}

/// Create a fresh temp file next to `final_path` for an atomic
/// write-then-rename. Uses `O_EXCL` (`create_new`) with a randomized suffix so a
/// symlink pre-created at the path cannot be followed (TOCTOU), and restricts
/// permissions to owner-only on Unix. Returns the open handle and its path.
pub fn create_temp_file(final_path: &Path) -> io::Result<(File, PathBuf)> {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    for _ in 0..8 {
        let suffix: u64 = rng.r#gen();
        let temp_path = temp_path_with_suffix(final_path, suffix);
        let mut opts = OpenOptions::new();
        opts.write(true).create_new(true);
        with_secure_mode(&mut opts);
        match opts.open(&temp_path) {
            Ok(f) => return Ok((f, temp_path)),
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(e) => return Err(e),
        }
    }
    Err(io::Error::new(
        io::ErrorKind::AlreadyExists,
        "could not create a unique temp file after several attempts",
    ))
}

fn temp_path_with_suffix(final_path: &Path, suffix: u64) -> PathBuf {
    let mut name = final_path
        .file_name()
        .map(|s| s.to_os_string())
        .unwrap_or_default();
    name.push(format!(".{:016x}.tmp", suffix));
    final_path.with_file_name(name)
}
