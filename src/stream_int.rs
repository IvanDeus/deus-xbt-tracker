//! Port of xbt/misc/stream_int.h (big-endian readers/writers).

/// `read_int(cb, r0)` — reads `cb` bytes big-endian; out-of-range reads yield 0
/// (like the data_ref overloads).
pub fn read_int(cb: usize, s: &[u8]) -> u64 {
    if s.len() < cb {
        return 0;
    }
    let mut v: u64 = 0;
    for b in &s[..cb] {
        v = v << 8 | *b as u64;
    }
    v
}

/// `read_int(cb, r, s_end)` — reads from `off`, bounded by the slice end.
pub fn read_int_at(cb: usize, s: &[u8], off: usize) -> u64 {
    if off >= s.len() {
        return 0;
    }
    read_int(cb, &s[off..])
}

/// `write_int(cb, w, v)` — writes the low `cb` bytes of v big-endian.
pub fn write_int(d: &mut [u8], off: usize, cb: usize, v: u64) {
    for i in 0..cb {
        d[off + cb - 1 - i] = (v >> (8 * i)) as u8;
    }
}

/// `read_int_le(cb, r0)`.
#[allow(dead_code)]
pub fn read_int_le(cb: usize, s: &[u8]) -> u64 {
    if s.len() < cb {
        return 0;
    }
    let mut v: u64 = 0;
    for i in (0..cb).rev() {
        v = v << 8 | s[i] as u64;
    }
    v
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_write_int() {
        let mut d = [0u8; 16];
        write_int(&mut d, 0, 8, 0x41727101980);
        assert_eq!(&d[..8], &[0, 0, 4, 0x17, 0x27, 0x10, 0x19, 0x80]);
        assert_eq!(read_int(8, &d), 0x41727101980);
        assert_eq!(read_int(4, &d[4..8]), 0x27101980);
        assert_eq!(read_int_at(4, &d, 14), 0); // not enough bytes
        assert_eq!(read_int_le(4, &[1, 2, 3, 4]), 0x04030201);
    }
}
