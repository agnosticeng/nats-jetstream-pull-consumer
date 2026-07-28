use crate::interceptor::Interceptor;
use async_nats::jetstream::Message;
use async_trait::async_trait;
use bytes::Bytes;

trait Decompressor: Send + Sync {
    fn decompress(&self, data: &[u8]) -> Result<Bytes, anyhow::Error>;
}

struct Lz4Decompressor;

impl Decompressor for Lz4Decompressor {
    fn decompress(&self, data: &[u8]) -> Result<Bytes, anyhow::Error> {
        lz4_flex::decompress_size_prepended(data)
            .map(Bytes::from)
            .map_err(Into::into)
    }
}

/// Interceptor that handles a 1-byte codec tag prefix on message payloads:
///
/// | Tag   | Meaning          |
/// |-------|------------------|
/// | `0x00`| Raw (pass-through, tag stripped) |
/// | `0x01`| LZ4 compressed (decompressed, tag stripped) |
pub struct DecompressInterceptor {
    decompressor: Box<dyn Decompressor>,
}

impl DecompressInterceptor {
    pub fn new() -> Self {
        Self {
            decompressor: Box::new(Lz4Decompressor),
        }
    }
}

impl Default for DecompressInterceptor {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Interceptor for DecompressInterceptor {
    async fn intercept(&self, msg: &mut Message) -> Result<(), anyhow::Error> {
        let data = &msg.message.payload;
        let (&tag, payload) = data
            .split_first()
            .ok_or_else(|| anyhow::anyhow!("empty payload"))?;

        msg.message.payload = match tag {
            0x00 => Bytes::copy_from_slice(payload),
            0x01 => self.decompressor.decompress(payload)?,
            other => {
                return Err(anyhow::anyhow!("unknown codec tag: 0x{other:02x}"));
            }
        };

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn test_new_creates_interceptor() {
        let interceptor = DecompressInterceptor::new();
        assert!(interceptor.decompressor.decompress(b"hello").is_err());
    }

    #[test]
    fn test_default_creates_interceptor() {
        let interceptor = DecompressInterceptor::default();
        assert!(interceptor.decompressor.decompress(b"test").is_err());
    }

    #[test]
    fn test_lz4_roundtrip() {
        let decompressor = Lz4Decompressor;
        let original = b"hello world";
        let compressed = lz4_flex::compress_prepend_size(original);
        let result = decompressor.decompress(&compressed).unwrap();
        assert_eq!(result, Bytes::from(&original[..]));
    }

    #[test]
    fn test_lz4_invalid_data() {
        let decompressor = Lz4Decompressor;
        let result = decompressor.decompress(b"invalid data");
        assert!(result.is_err());
    }
}
