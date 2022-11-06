use std::io;

#[allow(unused_imports)]
use crate::utils::invalid_data;

#[derive(Debug, Copy, Clone, strum::Display, strum::EnumString)]
#[strum(serialize_all = "lowercase")]
pub enum Compression {
    #[cfg(feature = "lz4")]
    Lz4,
    #[cfg(feature = "snappy")]
    Snappy,
}

impl Compression {
    #[allow(unreachable_code, unreachable_patterns, unused_variables)]
    pub fn compress(self, bytes: &[u8], header_size: usize) -> Vec<u8> {
        let max_size: usize = match self {
            #[cfg(feature = "lz4")]
            Compression::Lz4 => lz4_flex::block::get_maximum_output_size(bytes.len()),
            #[cfg(feature = "snappy")]
            Compression::Snappy => snap::raw::max_compress_len(bytes.len()),
            _ => panic!("no compression algorithm"),
        };
        let mut buffer = vec![0; header_size + max_size];
        let compressed_size: usize = match self {
            #[cfg(feature = "lz4")]
            Compression::Lz4 => lz4_flex::compress_into(bytes, &mut buffer[header_size..]).unwrap(),
            #[cfg(feature = "snappy")]
            Compression::Snappy => snap::raw::Encoder::new()
                .compress(bytes, &mut buffer[header_size..])
                .unwrap(),
            _ => panic!("no compression algorithm"),
        };
        buffer.truncate(header_size + compressed_size);
        buffer
    }

    #[allow(unreachable_patterns, unused_variables)]
    pub fn decompress(self, bytes: &[u8], uncompressed_size: usize) -> io::Result<Vec<u8>> {
        match self {
            #[cfg(feature = "lz4")]
            Compression::Lz4 => {
                lz4_flex::decompress(bytes, uncompressed_size).map_err(invalid_data)
            }
            #[cfg(feature = "snappy")]
            Compression::Snappy => panic!("unsupported"),
        }
    }

    #[allow(unreachable_patterns, unused_variables)]
    pub fn decompress_with_embedded_size(self, bytes: &[u8]) -> io::Result<Vec<u8>> {
        match self {
            #[cfg(feature = "lz4")]
            Compression::Lz4 => {
                let uncompressed_size =
                    u32::from_be_bytes(bytes[..4].try_into().map_err(invalid_data)?);
                lz4_flex::decompress(&bytes[4..], uncompressed_size as usize).map_err(invalid_data)
            }
            #[cfg(feature = "snappy")]
            Compression::Snappy => snap::raw::Decoder::new()
                .decompress_vec(bytes)
                .map_err(invalid_data),
        }
    }
}
