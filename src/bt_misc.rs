//! Port of xbt/misc/bt_misc.cpp + xbt/misc/xbt/string_view.h helpers.

/// Port of `to_int(std::string_view)`: std::from_chars over the *whole*
/// string; anything not fully parseable (incl. "+5", " 5", "5x", "", overflow)
/// yields 0.
pub fn to_int(s: &[u8]) -> i64 {
    let mut i = 0usize;
    let neg = if i < s.len() && s[i] == b'-' {
        i += 1;
        true
    } else {
        false
    };
    if i == s.len() {
        return 0;
    }
    let mut v: i128 = 0;
    while i < s.len() {
        let c = s[i];
        if !c.is_ascii_digit() {
            return 0;
        }
        v = v * 10 + (c - b'0') as i128;
        // std::from_chars returns result_out_of_range on overflow -> to_int yields 0
        if v > (1i128 << 63) {
            return 0;
        }
        i += 1;
    }
    let v = if neg { -v } else { v };
    if v < i64::MIN as i128 || v > i64::MAX as i128 {
        return 0;
    }
    v as i64
}

fn hex_decode_char(v: u8) -> i32 {
    match v {
        b'0'..=b'9' => (v - b'0') as i32,
        b'A'..=b'F' => (v - b'A') as i32 + 10,
        b'a'..=b'f' => (v - b'a') as i32 + 10,
        _ => -1,
    }
}

/// Port of `hex_decode(str_ref)`. Note: keeps the original's behavior of
/// producing `(-1 << 4) | -1` bytes for invalid hex digits.
pub fn hex_decode(v: &[u8]) -> Vec<u8> {
    let mut r = vec![0u8; v.len() >> 1];
    let mut i = 0;
    while i + 2 <= v.len() {
        let a = hex_decode_char(v[i]);
        let b = hex_decode_char(v[i + 1]);
        r[i >> 1] = (a << 4 | b) as u8;
        i += 2;
    }
    r
}

/// Port of `hex_encode(int l, int v)`: l lowercase hex digits of v.
pub fn hex_encode_int(l: usize, v: i64) -> String {
    let mut r = vec![b'0'; l];
    let mut v = v;
    let mut l = l;
    while l > 0 {
        l -= 1;
        r[l] = b"0123456789abcdef"[(v & 0xf) as usize];
        v >>= 4;
    }
    String::from_utf8(r).unwrap()
}

/// Port of `hex_encode(data_ref)`.
pub fn hex_encode(v: &[u8]) -> String {
    let mut r = String::with_capacity(v.len() * 2);
    for b in v {
        r.push_str(&hex_encode_int(2, *b as i64));
    }
    r
}

/// Port of `uri_decode(str_ref)`. Returns an empty string when a '%' escape is
/// truncated, exactly like the original.
pub fn uri_decode(v: &[u8]) -> Vec<u8> {
    let mut r = Vec::with_capacity(v.len());
    let mut i = 0usize;
    while i < v.len() {
        let c = v[i];
        match c {
            b'%' => {
                if i + 2 >= v.len() {
                    return Vec::new();
                }
                i += 1;
                let l = v[i] as i32;
                i += 1;
                let n = (hex_decode_char(l as u8) << 4 | hex_decode_char(v[i])) as i32;
                r.push(n as u8);
            }
            b'+' => r.push(b' '),
            _ => r.push(c),
        }
        i += 1;
    }
    r
}

/// Port of `uri_encode(str_ref)`.
pub fn uri_encode(v: &[u8]) -> String {
    let mut r = String::with_capacity(v.len());
    for &c in v {
        if c.is_ascii_alphabetic() || c.is_ascii_digit() {
            r.push(c as char);
        } else {
            match c {
                b' ' => r.push('+'),
                b'-' | b',' | b'.' | b'@' | b'_' => r.push(c as char),
                _ => {
                    r.push('%');
                    r.push_str(&hex_encode_int(2, c as i8 as i64));
                }
            }
        }
    }
    r
}

/// Port of `n2a(long long v, const char* postfix = NULL)`.
pub fn n2a(v0: i64) -> String {
    let mut d = String::new();
    let mut v = v0;
    if v < 0 {
        v = -v;
        d.push('-');
    }
    let mut l = 0usize;
    while v > 999999 {
        v /= 1000;
        l += 1;
    }
    if v > 999 {
        l += 1;
        let b = (v % 1000 / 10) as i64;
        v /= 1000;
        d.push_str(&v.to_string());
        if v < 10 && b % 10 != 0 {
            d.push_str(&format!(".{:02}", b));
        } else if v < 100 && b > 9 {
            d.push_str(&format!(".{}", b / 10));
        }
    } else {
        d.push_str(&v.to_string());
    }
    const A: [&str; 9] = ["", " k", " m", " g", " t", " p", " e", " z", " y"];
    d.push_str(A[l.min(8)]);
    d
}

/// Port of `duration2a(float)`.
pub fn duration2a(v0: f32) -> String {
    let v = v0;
    if v > 31557600.0 {
        format!("{:.1} years", v / 31557600.0)
    } else if v > 2629800.0 {
        format!("{:.1} months", v / 2629800.0)
    } else if v > 604800.0 {
        format!("{:.1} weeks", v / 604800.0)
    } else if v > 86400.0 {
        format!("{:.1} days", v / 86400.0)
    } else if v > 3600.0 {
        format!("{:.1} hours", v / 3600.0)
    } else if v > 60.0 {
        format!("{:.1} minutes", v / 60.0)
    } else {
        format!("{:.1} seconds", v)
    }
}

/// Port of `generate_random_string(int)`; uses the process PRNG (rng.rs),
/// which replaces C `rand()`.
pub fn generate_random_string(l: usize) -> String {
    const CHARS: &[u8; 62] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    let mut v = String::with_capacity(l);
    for _ in 0..l {
        v.push(CHARS[(crate::rng::rand() % 62) as usize] as char);
    }
    v
}

/// Port of `is_private_ipa(int)` (network-order int).
#[allow(dead_code)]
pub fn is_private_ipa(a: u32) -> bool {
    let h = a.to_be(); // ntohl
    (h & 0xff000000) == 0x0a000000
        || (h & 0xff000000) == 0x7f000000
        || (h & 0xfff00000) == 0xac100000
        || (h & 0xffff0000) == 0xc0a80000
}

/// Port of `xbt_syslog(const std::string&)` (POSIX branch).
pub fn xbt_syslog(msg: &str) {
    unsafe {
        let c = std::ffi::CString::new(msg).unwrap_or_else(|_| std::ffi::CString::new("log").unwrap());
        libc::syslog(libc::LOG_ERR, b"%s\0".as_ptr() as *const libc::c_char, c.as_ptr());
    }
}

/// inet_ntop(AF_INET, ...) equivalent.
pub fn inet_ntoa4(v: [u8; 4]) -> String {
    std::net::Ipv4Addr::from(v).to_string()
}

/// inet_ntop(AF_INET6, ...) equivalent.
pub fn inet_ntoa16(v: [u8; 16]) -> String {
    std::net::Ipv6Addr::from(v).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_int() {
        assert_eq!(to_int(b"123"), 123);
        assert_eq!(to_int(b"-123"), -123);
        assert_eq!(to_int(b"+123"), 0);
        assert_eq!(to_int(b"12x"), 0);
        assert_eq!(to_int(b""), 0);
        assert_eq!(to_int(b"9223372036854775807"), i64::MAX);
        assert_eq!(to_int(b"9223372036854775808"), 0);
        assert_eq!(to_int(b"-9223372036854775808"), i64::MIN);
    }

    #[test]
    fn test_uri() {
        assert_eq!(uri_decode(b"a%20b+c"), b"a b c");
        assert_eq!(uri_decode(b"%00%ff"), vec![0x00, 0xff]);
        assert_eq!(uri_decode(b"abc%2"), b"" as &[u8]); // truncated escape -> empty
        assert_eq!(uri_encode(b"a b%c-d.e_f@g"), "a+b%25c-d.e_f@g");
        assert_eq!(uri_encode(&[0x00, 0xff]), "%00%ff");
    }

    #[test]
    fn test_hex() {
        assert_eq!(hex_encode(&[0x00, 0xab, 0xff]), "00abff");
        assert_eq!(hex_decode(b"00abff"), vec![0x00, 0xab, 0xff]);
        assert_eq!(hex_encode_int(2, 0x1ff), "ff");
    }

    #[test]
    fn test_n2a() {
        assert_eq!(n2a(0), "0");
        assert_eq!(n2a(999), "999");
        assert_eq!(n2a(1000), "1 k");
        assert_eq!(n2a(1500), "1.5 k");
        assert_eq!(n2a(1099), "1.09 k");
        assert_eq!(n2a(999999), "999 k");
        assert_eq!(n2a(1000000), "1 m");
        assert_eq!(n2a(-1234), "-1.23 k");
    }

    #[test]
    fn test_duration2a() {
        assert_eq!(duration2a(30.0), "30.0 seconds");
        assert_eq!(duration2a(120.0), "2.0 minutes");
        assert_eq!(duration2a(804.0), "13.4 minutes");
        assert_eq!(duration2a(7200.0), "2.0 hours");
    }

    #[test]
    fn test_private() {
        assert!(is_private_ipa(u32::from_be(0x0a000001)));
        assert!(is_private_ipa(u32::from_be(0x7f000001)));
        assert!(!is_private_ipa(u32::from_be(0x08080808)));
    }
}
