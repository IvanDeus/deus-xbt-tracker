//! Port of xbt/misc/xcc_z.cpp `xcc_z::gzip`: manual gzip wrapper around a raw
//! DEFLATE stream, same header bytes as the original
//! (1f 8b 08 00 00 00 00 00 00 03) + crc32 + isize trailer.

use flate2::write::DeflateEncoder;
use flate2::Compression;
use std::io::Write;

pub fn gzip(s: &[u8]) -> Vec<u8> {
    let mut d: Vec<u8> = Vec::with_capacity(s.len() + (s.len() + 999) / 1000 + 12 + 10 + 8);
    d.extend_from_slice(&[0x1f, 0x8b, 0x08, 0, 0, 0, 0, 0, 0, 3]);
    let mut e = DeflateEncoder::new(Vec::new(), Compression::new(6));
    e.write_all(s).unwrap();
    d.extend_from_slice(&e.finish().unwrap());
    d.extend_from_slice(&crc32fast::hash(s).to_le_bytes());
    d.extend_from_slice(&(s.len() as u32).to_le_bytes());
    d
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;

    #[test]
    fn test_gzip_roundtrip() {
        let data = b"d5:filesd20:aaaaaaaaaaaaaaaaaaaad8:completei1e10:downloadedi2e10:incompletei3eeeee";
        let z = gzip(data);
        assert_eq!(&z[..4], &[0x1f, 0x8b, 0x08, 0x00]);
        assert_eq!(z[9], 3); // XOS = UNIX, like the original
        // verify with an independent decoder (flate2 multi-gzip reader)
        let mut out = Vec::new();
        flate2::read::GzDecoder::new(&z[..]).read_to_end(&mut out).unwrap();
        assert_eq!(out, data);
        // trailer check
        let n = z.len();
        assert_eq!(&z[n - 8..n - 4], &crc32fast::hash(data).to_le_bytes());
        assert_eq!(&z[n - 4..], &(data.len() as u32).to_le_bytes());
    }
}
