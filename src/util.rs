//! Small helpers shared by every module: Redis-exact number parsing and
//! formatting, glob matching, clocks and a fast thread-local PRNG.
//!
//! Number parsing and formatting deliberately mirror Redis' `util.c`
//! (`string2ll`, `string2d`, `d2string`) because clients and the differential
//! test suite observe the exact bytes.

use std::cell::Cell;
use std::time::{SystemTime, UNIX_EPOCH};

/// Wall-clock milliseconds since the Unix epoch. Expiry deadlines are stored in
/// this clock, like Redis, so they survive restarts and can be replicated.
pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Wall-clock microseconds since the Unix epoch.
pub fn now_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0)
}

/// A cheap monotonic timestamp in platform ticks, used to time every command
/// (for SLOWLOG and INFO commandstats). On macOS this is `mach_absolute_time`
/// without the per-call timebase conversion `Instant::now` performs.
#[inline]
pub fn ticks() -> u64 {
    #[cfg(target_os = "macos")]
    {
        // SAFETY: mach_absolute_time has no preconditions and no side effects.
        #[allow(deprecated)]
        unsafe {
            libc::mach_absolute_time()
        }
    }
    #[cfg(not(target_os = "macos"))]
    {
        static START: std::sync::OnceLock<std::time::Instant> = std::sync::OnceLock::new();
        START
            .get_or_init(std::time::Instant::now)
            .elapsed()
            .as_nanos() as u64
    }
}

/// Converts a [`ticks`] difference to microseconds.
#[inline]
pub fn ticks_to_us(t: u64) -> u64 {
    #[cfg(target_os = "macos")]
    {
        static TB: std::sync::OnceLock<(u64, u64)> = std::sync::OnceLock::new();
        #[allow(deprecated)]
        let (numer, denom) = *TB.get_or_init(|| {
            let mut info = libc::mach_timebase_info { numer: 0, denom: 0 };
            // SAFETY: `info` is a valid, writable mach_timebase_info.
            #[allow(deprecated)]
            unsafe {
                libc::mach_timebase_info(&mut info)
            };
            (info.numer.max(1) as u64, info.denom.max(1) as u64)
        });
        (t as u128 * numer as u128 / denom as u128 / 1000) as u64
    }
    #[cfg(not(target_os = "macos"))]
    {
        t / 1000
    }
}

/// Parses a signed 64-bit integer with the exact rules of Redis' `string2ll`:
/// no leading `+`, no leading zeros, no whitespace, `-0` rejected.
pub fn parse_i64(s: &[u8]) -> Option<i64> {
    if s.is_empty() || s.len() > 20 {
        return None;
    }
    if s.len() == 1 && s[0] == b'0' {
        return Some(0);
    }
    let (neg, digits) = if s[0] == b'-' {
        (true, &s[1..])
    } else {
        (false, s)
    };
    if digits.is_empty() || !(b'1'..=b'9').contains(&digits[0]) {
        return None;
    }
    let mut v: u64 = 0;
    for &c in digits {
        if !c.is_ascii_digit() {
            return None;
        }
        v = v.checked_mul(10)?.checked_add((c - b'0') as u64)?;
    }
    if neg {
        if v > (i64::MAX as u64) + 1 {
            None
        } else {
            Some((v as i64).wrapping_neg())
        }
    } else if v > i64::MAX as u64 {
        None
    } else {
        Some(v as i64)
    }
}

/// Parses an unsigned integer with `string2ll` rules (rejects negatives).
pub fn parse_u64(s: &[u8]) -> Option<u64> {
    parse_i64(s).and_then(|v| u64::try_from(v).ok())
}

/// Parses a double with the rules of Redis' `string2d` (a strict `strtod`):
/// no leading whitespace, no trailing garbage, NaN rejected, overflow and
/// underflow rejected. Accepts `inf`, `+inf`, `-inf`, `infinity`.
pub fn parse_f64(s: &[u8]) -> Option<f64> {
    if s.is_empty() || s[0].is_ascii_whitespace() {
        return None;
    }
    parse_f64_lenient_prefix(s)
}

/// Like [`parse_f64`] but follows `zslParseRange`, which calls raw `strtod`
/// and therefore tolerates leading whitespace.
pub fn parse_f64_range(s: &[u8]) -> Option<f64> {
    let start = s
        .iter()
        .position(|c| !c.is_ascii_whitespace())
        .unwrap_or(s.len());
    let t = &s[start..];
    if t.is_empty() {
        return None;
    }
    let v = parse_f64_lenient_prefix(t)?;
    Some(v)
}

fn parse_f64_lenient_prefix(s: &[u8]) -> Option<f64> {
    let text = std::str::from_utf8(s).ok()?;
    // Rust's parser accepts things strtod does not ("infinity" is fine for
    // both; "_" never parses). Reject an explicit NaN spelling up front.
    let lower = text.to_ascii_lowercase();
    let body = lower.trim_start_matches(['+', '-']);
    if body.starts_with("nan") {
        return None;
    }
    if body.starts_with("0x") {
        return parse_hex_float(&lower);
    }
    let v: f64 = text.parse().ok()?;
    if v.is_nan() {
        return None;
    }
    if v.is_infinite() && !body.starts_with("inf") {
        return None; // overflow: strtod sets ERANGE + HUGE_VAL
    }
    if v == 0.0 && body.bytes().any(|c| (b'1'..=b'9').contains(&c)) {
        // Underflow to zero ("1e-400") also sets ERANGE. Only digits in the
        // mantissa count; "0e5" is a genuine zero.
        let mantissa = body.split('e').next().unwrap_or("");
        if mantissa.bytes().any(|c| (b'1'..=b'9').contains(&c)) {
            return None;
        }
    }
    Some(v)
}

/// Minimal C99 hex-float support (`0x1.8p3`) so `strtod`-accepted inputs keep
/// working. Rare in practice, but cheap to support.
fn parse_hex_float(s: &str) -> Option<f64> {
    let (neg, rest) = match s.as_bytes().first()? {
        b'-' => (true, &s[1..]),
        b'+' => (false, &s[1..]),
        _ => (false, s),
    };
    let rest = rest.strip_prefix("0x")?;
    let (mant, exp) = match rest.find('p') {
        Some(i) => (&rest[..i], rest[i + 1..].parse::<i32>().ok()?),
        None => (rest, 0),
    };
    let (int_part, frac_part) = match mant.find('.') {
        Some(i) => (&mant[..i], &mant[i + 1..]),
        None => (mant, ""),
    };
    if int_part.is_empty() && frac_part.is_empty() {
        return None;
    }
    let mut v = 0f64;
    for c in int_part.chars() {
        v = v * 16.0 + c.to_digit(16)? as f64;
    }
    let mut scale = 1.0 / 16.0;
    for c in frac_part.chars() {
        v += c.to_digit(16)? as f64 * scale;
        scale /= 16.0;
    }
    let v = v * 2f64.powi(exp);
    if v.is_infinite() {
        return None;
    }
    Some(if neg { -v } else { v })
}

/// Formats an integer into `buf` and returns the written slice.
pub fn fmt_i64(v: i64, buf: &mut itoa::Buffer) -> &[u8] {
    buf.format(v).as_bytes()
}

/// Splits `v` (finite, non-zero) into its shortest round-trip decimal digits
/// and the exponent `K` such that `v = digits * 10^K`.
fn shortest_digits(v: f64) -> (Vec<u8>, i32) {
    // `{:e}` prints the shortest representation that round-trips, e.g.
    // "1.2345e-7". Parse it back into digits + exponent.
    let s = format!("{:e}", v.abs());
    let (mant, exp) = s.split_once('e').expect("{:e} always has an exponent");
    let exp: i32 = exp.parse().expect("valid exponent");
    let mut digits: Vec<u8> = mant.bytes().filter(|c| *c != b'.').collect();
    while digits.len() > 1 && *digits.last().unwrap() == b'0' {
        digits.pop();
    }
    let k = exp - (digits.len() as i32 - 1);
    (digits, k)
}

/// Formats a double exactly like Redis' `d2string` (integers printed as such,
/// otherwise the fpconv shortest representation with its exponent rules).
/// Used for scores and every RESP double reply.
pub fn fmt_f64(v: f64) -> String {
    if v.is_nan() {
        return "nan".into();
    }
    if v.is_infinite() {
        return if v < 0.0 { "-inf".into() } else { "inf".into() };
    }
    if v == 0.0 {
        return if v.is_sign_negative() {
            "-0".into()
        } else {
            "0".into()
        };
    }
    const HALF: f64 = (i64::MAX / 2) as f64;
    if (-HALF..=HALF).contains(&v) && v.fract() == 0.0 {
        return (v as i64).to_string();
    }
    let neg = v < 0.0;
    let (digits, k) = shortest_digits(v);
    let mut out = String::with_capacity(24);
    if neg {
        out.push('-');
    }
    let nd = digits.len() as i32;
    let exp = (k + nd - 1).abs();
    let dstr = |d: &[u8]| String::from_utf8_lossy(d).into_owned();
    if k >= 0 && exp < nd + 7 {
        out.push_str(&dstr(&digits));
        for _ in 0..k {
            out.push('0');
        }
        return out;
    }
    if k < 0 && (k > -7 || exp < 4) {
        let offset = nd - k.abs();
        if offset <= 0 {
            out.push_str("0.");
            for _ in 0..(-offset) {
                out.push('0');
            }
            out.push_str(&dstr(&digits));
        } else {
            out.push_str(&dstr(&digits[..offset as usize]));
            out.push('.');
            out.push_str(&dstr(&digits[offset as usize..]));
        }
        return out;
    }
    // Scientific notation, fpconv style: "1.5e+20", "1e-07" is "1e-7".
    let nd2 = nd.min(18 - neg as i32) as usize;
    out.push(digits[0] as char);
    if nd2 > 1 {
        out.push('.');
        out.push_str(&dstr(&digits[1..nd2]));
    }
    out.push('e');
    out.push(if k + nd - 1 < 0 { '-' } else { '+' });
    out.push_str(&exp.to_string());
    out
}

/// Human-friendly formatting used by `INCRBYFLOAT`/`HINCRBYFLOAT`: plain
/// decimal notation (never an exponent) with the shortest digits that round
/// trip. Redis uses `%.17Lf` + trailing-zero trimming, which on 80-bit long
/// double platforms prints the same thing for typical inputs.
pub fn fmt_f64_human(v: f64) -> String {
    if v == 0.0 {
        return "0".into();
    }
    if v.fract() == 0.0 && v.abs() < 1e17 {
        return (v as i64).to_string();
    }
    let neg = v < 0.0;
    let (digits, k) = shortest_digits(v);
    let mut out = String::new();
    if neg {
        out.push('-');
    }
    let nd = digits.len() as i32;
    let dstr = |d: &[u8]| String::from_utf8_lossy(d).into_owned();
    if k >= 0 {
        out.push_str(&dstr(&digits));
        for _ in 0..k {
            out.push('0');
        }
    } else {
        let offset = nd + k;
        if offset <= 0 {
            out.push_str("0.");
            for _ in 0..(-offset) {
                out.push('0');
            }
            out.push_str(&dstr(&digits));
        } else {
            out.push_str(&dstr(&digits[..offset as usize]));
            out.push('.');
            out.push_str(&dstr(&digits[offset as usize..]));
        }
    }
    out
}

/// Case-insensitive ASCII comparison of a command argument with a keyword.
#[inline]
pub fn eq_ic(a: &[u8], kw: &str) -> bool {
    a.eq_ignore_ascii_case(kw.as_bytes())
}

/// Redis' glob matcher (`stringmatchlen`): `*`, `?`, `[...]` with ranges and
/// negation, and `\` escapes. Ported 1:1 including the early-exit
/// optimization that keeps pathological patterns linear-ish.
pub fn glob_match(pattern: &[u8], string: &[u8], nocase: bool) -> bool {
    let mut skip_longer = false;
    glob_impl(pattern, string, nocase, &mut skip_longer, 0)
}

fn glob_impl(pat: &[u8], s: &[u8], nocase: bool, skip_longer: &mut bool, nesting: u32) -> bool {
    if nesting > 1000 {
        return false;
    }
    let pc = |i: usize| -> u8 { pat.get(i).copied().unwrap_or(0) };
    let fold = |c: u8| if nocase { c.to_ascii_lowercase() } else { c };
    let (mut p, mut si) = (0usize, 0usize);
    while p < pat.len() && si < s.len() {
        match pat[p] {
            b'*' => {
                while p + 1 < pat.len() && pat[p + 1] == b'*' {
                    p += 1;
                }
                if p + 1 == pat.len() {
                    return true;
                }
                while si < s.len() {
                    if glob_impl(&pat[p + 1..], &s[si..], nocase, skip_longer, nesting + 1) {
                        return true;
                    }
                    if *skip_longer {
                        return false;
                    }
                    si += 1;
                }
                *skip_longer = true;
                return false;
            }
            b'?' => {
                si += 1;
            }
            b'[' => {
                p += 1;
                let not = pc(p) == b'^';
                if not {
                    p += 1;
                }
                let mut matched = false;
                loop {
                    if pc(p) == b'\\' && pat.len() - p >= 2 {
                        p += 1;
                        if pat[p] == s[si] {
                            matched = true;
                        }
                    } else if pc(p) == b']' && p < pat.len() {
                        break;
                    } else if p >= pat.len() {
                        p -= 1;
                        break;
                    } else if pat.len() - p >= 3 && pat[p + 1] == b'-' {
                        let (mut start, mut end) = (pat[p], pat[p + 2]);
                        let mut c = s[si];
                        if start > end {
                            std::mem::swap(&mut start, &mut end);
                        }
                        if nocase {
                            start = start.to_ascii_lowercase();
                            end = end.to_ascii_lowercase();
                            c = c.to_ascii_lowercase();
                        }
                        p += 2;
                        if c >= start && c <= end {
                            matched = true;
                        }
                    } else if fold(pat[p]) == fold(s[si]) {
                        matched = true;
                    }
                    p += 1;
                }
                if not {
                    matched = !matched;
                }
                if !matched {
                    return false;
                }
                si += 1;
            }
            c => {
                let c = if c == b'\\' && pat.len() - p >= 2 {
                    p += 1;
                    pat[p]
                } else {
                    c
                };
                if fold(c) != fold(s[si]) {
                    return false;
                }
                si += 1;
            }
        }
        p += 1;
        if si == s.len() {
            while p < pat.len() && pat[p] == b'*' {
                p += 1;
            }
            break;
        }
    }
    p >= pat.len() && si == s.len()
}

thread_local! {
    static RNG: Cell<u64> = Cell::new(seed_from_env());
}

fn seed_from_env() -> u64 {
    let t = now_us();
    let addr = &t as *const u64 as u64;
    let mut x = t ^ addr.rotate_left(29) ^ 0x9E37_79B9_7F4A_7C15;
    if x == 0 {
        x = 1;
    }
    x
}

/// A fast thread-local pseudo random number (wyrand). Not cryptographic; used
/// for skiplist levels, sampling and SPOP/SRANDMEMBER.
#[inline]
pub fn rand_u64() -> u64 {
    RNG.with(|r| {
        let s = r.get().wrapping_add(0xA076_1D64_78BD_642F);
        r.set(s);
        let t = (s as u128).wrapping_mul((s ^ 0xE703_7ED1_A0B4_28DB) as u128);
        ((t >> 64) as u64) ^ (t as u64)
    })
}

/// Uniform random number in `0..n` (`n > 0`).
#[inline]
pub fn rand_below(n: u64) -> u64 {
    debug_assert!(n > 0);
    ((rand_u64() as u128 * n as u128) >> 64) as u64
}

/// A tiny deterministic PRNG for reproducible sequences (tests, generators).
#[derive(Clone, Debug)]
pub struct SeededRng(u64);

impl SeededRng {
    /// Creates a generator from a seed.
    pub fn new(seed: u64) -> Self {
        SeededRng(seed ^ 0x2545_F491_4F6C_DD1D)
    }
    /// Next 64 random bits.
    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0xA076_1D64_78BD_642F);
        let t = (self.0 as u128).wrapping_mul((self.0 ^ 0xE703_7ED1_A0B4_28DB) as u128);
        ((t >> 64) as u64) ^ (t as u64)
    }
    /// Uniform number in `0..n`.
    pub fn below(&mut self, n: u64) -> u64 {
        ((self.next_u64() as u128 * n as u128) >> 64) as u64
    }
}

/// Converts a Redis-style `[start, end]` inclusive index pair (negative values
/// count from the end) into a clamped half-open range, or `None` when empty.
pub fn normalize_range(start: i64, end: i64, len: usize) -> Option<(usize, usize)> {
    let len = len as i64;
    let mut s = if start < 0 { len + start } else { start };
    let mut e = if end < 0 { len + end } else { end };
    if s < 0 {
        s = 0;
    }
    if s > e || s >= len {
        return None;
    }
    if e >= len {
        e = len - 1;
    }
    Some((s as usize, e as usize + 1))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn string2ll_rules() {
        assert_eq!(parse_i64(b"0"), Some(0));
        assert_eq!(parse_i64(b"-1"), Some(-1));
        assert_eq!(parse_i64(b"9223372036854775807"), Some(i64::MAX));
        assert_eq!(parse_i64(b"-9223372036854775808"), Some(i64::MIN));
        assert_eq!(parse_i64(b"9223372036854775808"), None);
        for bad in [
            &b""[..],
            b"+1",
            b"01",
            b"-0",
            b" 1",
            b"1 ",
            b"1.0",
            b"-",
            b"abc",
        ] {
            assert_eq!(parse_i64(bad), None, "{:?}", String::from_utf8_lossy(bad));
        }
    }

    #[test]
    fn string2d_rules() {
        assert_eq!(parse_f64(b"1.5"), Some(1.5));
        assert_eq!(parse_f64(b"-inf"), Some(f64::NEG_INFINITY));
        assert_eq!(parse_f64(b"+inf"), Some(f64::INFINITY));
        assert_eq!(parse_f64(b"1e3"), Some(1000.0));
        assert_eq!(parse_f64(b".5"), Some(0.5));
        assert_eq!(parse_f64(b"0x10"), Some(16.0));
        assert_eq!(parse_f64(b"0e10"), Some(0.0));
        for bad in [&b""[..], b" 1", b"1 ", b"nan", b"abc", b"1e400", b"1e-400"] {
            assert_eq!(parse_f64(bad), None, "{:?}", String::from_utf8_lossy(bad));
        }
        assert_eq!(parse_f64_range(b" 1"), Some(1.0));
    }

    #[test]
    fn d2string_matches_redis() {
        let cases: &[(f64, &str)] = &[
            (1.0, "1"),
            (-3.0, "-3"),
            (1.5, "1.5"),
            (0.1, "0.1"),
            (-0.0, "-0"),
            (1e20, "1e+20"),
            (1e15, "1000000000000000"),
            (1.5e10, "15000000000"),
            (1.5e-7, "1.5e-7"),
            (123456.789, "123456.789"),
            (0.001, "0.001"),
            (1e-7, "1e-7"),
            (2.71, "2.71"),
            (f64::INFINITY, "inf"),
            (1e300, "1e+300"),
            (2.5e-5, "0.000025"),
        ];
        for &(v, want) in cases {
            assert_eq!(fmt_f64(v), want, "value {v:e}");
        }
    }

    #[test]
    fn human_float() {
        assert_eq!(fmt_f64_human(10.5 + 0.1), "10.6");
        assert_eq!(fmt_f64_human(3.0), "3");
        assert_eq!(fmt_f64_human(-0.25), "-0.25");
        assert_eq!(fmt_f64_human(1e20), "100000000000000000000");
        assert_eq!(fmt_f64_human(1.5e-7), "0.00000015");
    }

    #[test]
    fn glob() {
        assert!(glob_match(b"*", b"anything", false));
        assert!(glob_match(b"h?llo", b"hello", false));
        assert!(glob_match(b"h[ae]llo", b"hallo", false));
        assert!(!glob_match(b"h[^e]llo", b"hello", false));
        assert!(glob_match(b"h[a-b]llo", b"hbllo", false));
        assert!(glob_match(b"user:*:name", b"user:42:name", false));
        assert!(!glob_match(b"user:*:name", b"user:42:age", false));
        assert!(glob_match(b"a\\*b", b"a*b", false));
        assert!(glob_match(b"HELLO", b"hello", true));
        assert!(glob_match(b"a*", b"a", false));
        assert!(!glob_match(b"a*b", b"a", false));
        assert!(glob_match(b"[abc", b"a", false));
        // Pathological pattern must terminate quickly.
        let s = vec![b'a'; 60];
        assert!(!glob_match(b"a*a*a*a*a*a*a*a*a*a*b", &s, false));
    }

    #[test]
    fn ranges() {
        assert_eq!(normalize_range(0, -1, 5), Some((0, 5)));
        assert_eq!(normalize_range(-2, -1, 5), Some((3, 5)));
        assert_eq!(normalize_range(3, 1, 5), None);
        assert_eq!(normalize_range(-100, 100, 5), Some((0, 5)));
        assert_eq!(normalize_range(5, 10, 5), None);
        assert_eq!(normalize_range(0, 0, 0), None);
    }
}
