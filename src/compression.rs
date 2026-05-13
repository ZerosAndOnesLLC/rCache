use bytes::Bytes;

/// Magic header for LZ4-compressed values.
const LZ4_MAGIC: &[u8; 4] = b"LZ4C";

/// Check if a value has the LZ4 compression header.
pub fn is_compressed(data: &[u8]) -> bool {
    data.len() > 8 && &data[0..4] == LZ4_MAGIC
}

/// Compress a value with LZ4, prepending the magic header and original length.
/// Returns the compressed bytes with header: "LZ4C" + original_len(u32 LE) + compressed_data.
pub fn compress(data: &[u8]) -> Bytes {
    let compressed = lz4_flex::compress_prepend_size(data);
    let mut result = Vec::with_capacity(8 + compressed.len());
    result.extend_from_slice(LZ4_MAGIC);
    result.extend_from_slice(&(data.len() as u32).to_le_bytes());
    result.extend_from_slice(&compressed);
    Bytes::from(result)
}

fn read_original_len(data: &[u8]) -> Result<usize, &'static str> {
    let bytes: [u8; 4] = data.get(4..8)
        .ok_or("data too short for LZ4 header")?
        .try_into()
        .map_err(|_| "data too short for LZ4 header")?;
    Ok(u32::from_le_bytes(bytes) as usize)
}

/// Decompress a value that has the LZ4 magic header.
/// Returns the original uncompressed data.
pub fn decompress(data: &[u8]) -> Result<Bytes, String> {
    if data.len() < 8 {
        return Err("Data too short for LZ4 header".to_string());
    }
    if &data[0..4] != LZ4_MAGIC {
        return Err("Missing LZ4 magic header".to_string());
    }
    let original_len = read_original_len(data).map_err(|e| e.to_string())?;
    let compressed_data = &data[8..];

    match lz4_flex::decompress_size_prepended(compressed_data) {
        Ok(decompressed) => {
            if decompressed.len() != original_len {
                return Err(format!(
                    "Decompressed size mismatch: expected {}, got {}",
                    original_len,
                    decompressed.len()
                ));
            }
            Ok(Bytes::from(decompressed))
        }
        Err(e) => Err(format!("LZ4 decompression error: {}", e)),
    }
}

/// Get the original (uncompressed) size of a potentially compressed value.
pub fn original_size(data: &[u8]) -> usize {
    if is_compressed(data) {
        read_original_len(data).unwrap_or(data.len())
    } else {
        data.len()
    }
}

/// Maybe compress a value based on threshold.
/// If compression is enabled and data exceeds threshold, compress it.
/// Otherwise return the original data unchanged.
pub fn maybe_compress(data: &[u8], enabled: bool, threshold: usize) -> Bytes {
    if enabled && data.len() > threshold && !is_compressed(data) {
        compress(data)
    } else {
        Bytes::copy_from_slice(data)
    }
}

/// Transparently decompress a value if it has the LZ4 header.
/// Returns an error if decompression fails — callers should propagate it rather than
/// silently treating compressed bytes as cleartext.
pub fn maybe_decompress(data: &Bytes) -> Result<Bytes, String> {
    if is_compressed(data) {
        decompress(data)
    } else {
        Ok(data.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compress_decompress() {
        let data = b"Hello, this is a test string that should be compressed!";
        let compressed = compress(data);
        assert!(is_compressed(&compressed));
        let decompressed = decompress(&compressed).unwrap();
        assert_eq!(decompressed.as_ref(), data);
    }

    #[test]
    fn test_original_size() {
        let data = b"Hello, world!";
        let compressed = compress(data);
        assert_eq!(original_size(&compressed), data.len());
        assert_eq!(original_size(data), data.len());
    }

    #[test]
    fn test_maybe_compress_below_threshold() {
        let data = b"short";
        let result = maybe_compress(data, true, 1024);
        assert!(!is_compressed(&result));
        assert_eq!(result.as_ref(), data);
    }

    #[test]
    fn test_maybe_compress_above_threshold() {
        let data = vec![b'A'; 2048];
        let result = maybe_compress(&data, true, 1024);
        assert!(is_compressed(&result));
        let decompressed = maybe_decompress(&result).unwrap();
        assert_eq!(decompressed.as_ref(), data.as_slice());
    }

    #[test]
    fn test_maybe_compress_disabled() {
        let data = vec![b'A'; 2048];
        let result = maybe_compress(&data, false, 1024);
        assert!(!is_compressed(&result));
    }

    #[test]
    fn test_maybe_decompress_not_compressed() {
        let data = Bytes::from("not compressed");
        let result = maybe_decompress(&data).unwrap();
        assert_eq!(result, data);
    }

    #[test]
    fn test_short_data_no_panic() {
        // Data with the LZ4 magic but truncated must not panic
        let data = Bytes::from_static(b"LZ4C");
        let result = maybe_decompress(&data).unwrap();
        // 4 bytes < 8 so is_compressed returns false, passthrough
        assert_eq!(result, data);

        // 8 bytes exactly — is_compressed requires >8, so still passthrough
        let data = Bytes::from_static(b"LZ4Cabcd");
        let result = maybe_decompress(&data).unwrap();
        assert_eq!(result, data);
    }
}
