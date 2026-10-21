//! RESP2/RESP3 wire protocol: an incremental request parser and a reply
//! encoder.
//!
//! The parser accepts both request forms Redis accepts:
//!
//! * **multibulk** — `*<n>\r\n$<len>\r\n<bytes>\r\n...` (what clients send);
//! * **inline** — a plain text line split on whitespace with Redis' quoting
//!   rules (what you type into `telnet`).
//!
//! Parsing is zero-copy for multibulk requests: arguments are returned as
//! offsets into the connection's input buffer. The parser is resumable, so a
//! large pipelined request that arrives across many TCP reads is scanned once,
//! not once per read.
//!
//! The encoder ([`Reply`]) writes into a connection's output buffer and knows
//! how each logical type maps onto RESP2 and RESP3 (maps, sets, doubles,
//! nulls, booleans, verbatim strings, push messages).

use memchr::memchr;

/// Maximum size of an inline request line (`PROTO_INLINE_MAX_SIZE`).
pub const INLINE_MAX: usize = 64 * 1024;
/// Maximum number of arguments in a multibulk request.
pub const MAX_MULTIBULK: usize = 1024 * 1024;
/// Maximum size of a single bulk argument (`proto-max-bulk-len`).
pub const MAX_BULK: usize = 512 * 1024 * 1024;

/// A fatal protocol violation. The connection is answered with
/// `-ERR Protocol error: ...` and closed, like Redis does.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProtocolError(pub String);

/// Where the arguments of a parsed request live.
#[derive(Debug, PartialEq, Eq)]
pub enum Args {
    /// `(start, len)` offsets into the input buffer.
    Ranges(Vec<(usize, usize)>),
    /// Owned arguments (inline requests, which may contain escapes).
    Owned(Vec<Vec<u8>>),
}

/// Incremental request parser state for one connection.
#[derive(Default, Debug)]
pub struct Parser {
    /// Remaining bulk arguments of the multibulk request in progress.
    pending: Option<usize>,
    ranges: Vec<(usize, usize)>,
}

fn parse_len(line: &[u8]) -> Option<i64> {
    // Redis uses string2ll here: strict integers.
    crate::util::parse_i64(line)
}

impl Parser {
    /// A fresh parser.
    pub fn new() -> Self {
        Self::default()
    }

    /// True when a multibulk request has been partially parsed.
    pub fn in_progress(&self) -> bool {
        self.pending.is_some()
    }

    /// Tries to parse one request starting at `*pos` in `buf`. On success the
    /// request's arguments are returned and `*pos` advances past it. Returns
    /// `Ok(None)` when more bytes are needed (`*pos` may advance past the parts
    /// already parsed). An empty `Args` means "no command" (empty line or
    /// `*0`), which callers skip.
    pub fn parse(&mut self, buf: &[u8], pos: &mut usize) -> Result<Option<Args>, ProtocolError> {
        if self.pending.is_none() {
            if *pos >= buf.len() {
                return Ok(None);
            }
            if buf[*pos] != b'*' {
                return self.parse_inline(buf, pos);
            }
            let Some(nl) = memchr(b'\r', &buf[*pos..]) else {
                if buf.len() - *pos > INLINE_MAX {
                    return Err(ProtocolError("too big mbulk count string".into()));
                }
                return Ok(None);
            };
            let line_end = *pos + nl;
            if line_end + 1 >= buf.len() {
                return Ok(None);
            }
            let n = parse_len(&buf[*pos + 1..line_end])
                .filter(|n| *n <= MAX_MULTIBULK as i64)
                .ok_or_else(|| ProtocolError("invalid multibulk length".into()))?;
            *pos = line_end + 2;
            if n <= 0 {
                return Ok(Some(Args::Ranges(Vec::new())));
            }
            self.pending = Some(n as usize);
            self.ranges.clear();
            self.ranges.reserve(n.min(1024) as usize);
        }
        while let Some(remaining) = self.pending {
            if remaining == 0 {
                self.pending = None;
                return Ok(Some(Args::Ranges(std::mem::take(&mut self.ranges))));
            }
            if *pos >= buf.len() {
                return Ok(None);
            }
            if buf[*pos] != b'$' {
                let got = buf[*pos] as char;
                return Err(ProtocolError(format!("expected '$', got '{got}'")));
            }
            let Some(nl) = memchr(b'\r', &buf[*pos..]) else {
                if buf.len() - *pos > INLINE_MAX {
                    return Err(ProtocolError("too big bulk count string".into()));
                }
                return Ok(None);
            };
            let line_end = *pos + nl;
            let len = parse_len(&buf[*pos + 1..line_end])
                .filter(|n| *n >= 0 && *n <= MAX_BULK as i64)
                .ok_or_else(|| ProtocolError("invalid bulk length".into()))?
                as usize;
            let data_start = line_end + 2;
            if data_start + len + 2 > buf.len() {
                return Ok(None);
            }
            self.ranges.push((data_start, len));
            *pos = data_start + len + 2;
            self.pending = Some(remaining - 1);
        }
        unreachable!("loop returns when the request completes")
    }

    fn parse_inline(&mut self, buf: &[u8], pos: &mut usize) -> Result<Option<Args>, ProtocolError> {
        let Some(nl) = memchr(b'\n', &buf[*pos..]) else {
            if buf.len() - *pos > INLINE_MAX {
                return Err(ProtocolError("too big inline request".into()));
            }
            return Ok(None);
        };
        let mut line = &buf[*pos..*pos + nl];
        if line.last() == Some(&b'\r') {
            line = &line[..line.len() - 1];
        }
        let args =
            split_args(line).ok_or_else(|| ProtocolError("unbalanced quotes in request".into()))?;
        *pos += nl + 1;
        Ok(Some(Args::Owned(args)))
    }
}

fn hex_val(c: u8) -> u8 {
    match c {
        b'0'..=b'9' => c - b'0',
        b'a'..=b'f' => c - b'a' + 10,
        b'A'..=b'F' => c - b'A' + 10,
        _ => 0,
    }
}

/// Splits an inline request with Redis' `sdssplitargs` rules: whitespace
/// separates arguments, `"..."` supports `\n \r \t \b \a \\ \" \xHH` escapes,
/// `'...'` supports `\'`. Returns `None` on unbalanced quotes.
pub fn split_args(line: &[u8]) -> Option<Vec<Vec<u8>>> {
    let mut out = Vec::new();
    let mut i = 0;
    let n = line.len();
    loop {
        while i < n && line[i].is_ascii_whitespace() {
            i += 1;
        }
        if i >= n {
            return Some(out);
        }
        let mut cur = Vec::new();
        let (mut inq, mut insq) = (false, false);
        loop {
            if inq {
                if i >= n {
                    return None;
                }
                if line[i] == b'\\'
                    && i + 3 < n
                    && line[i + 1] == b'x'
                    && line[i + 2].is_ascii_hexdigit()
                    && line[i + 3].is_ascii_hexdigit()
                {
                    cur.push(hex_val(line[i + 2]) * 16 + hex_val(line[i + 3]));
                    i += 3;
                } else if line[i] == b'\\' && i + 1 < n {
                    i += 1;
                    cur.push(match line[i] {
                        b'n' => b'\n',
                        b'r' => b'\r',
                        b't' => b'\t',
                        b'b' => 8,
                        b'a' => 7,
                        c => c,
                    });
                } else if line[i] == b'"' {
                    // Closing quote must be followed by a space or the end.
                    if i + 1 < n && !line[i + 1].is_ascii_whitespace() {
                        return None;
                    }
                    inq = false;
                    i += 1;
                    break;
                } else {
                    cur.push(line[i]);
                }
            } else if insq {
                if i >= n {
                    return None;
                }
                if line[i] == b'\\' && i + 1 < n && line[i + 1] == b'\'' {
                    i += 1;
                    cur.push(b'\'');
                } else if line[i] == b'\'' {
                    if i + 1 < n && !line[i + 1].is_ascii_whitespace() {
                        return None;
                    }
                    insq = false;
                    i += 1;
                    break;
                } else {
                    cur.push(line[i]);
                }
            } else {
                if i >= n {
                    break;
                }
                match line[i] {
                    b' ' | b'\n' | b'\r' | b'\t' | 0 => break,
                    b'"' => inq = true,
                    b'\'' => insq = true,
                    c => cur.push(c),
                }
            }
            i += 1;
        }
        let _ = (inq, insq);
        out.push(cur);
    }
}

/// Encodes replies for one client, honoring its protocol version.
pub struct Reply<'a> {
    /// The output buffer being appended to.
    pub buf: &'a mut Vec<u8>,
    /// True for RESP3 clients.
    pub resp3: bool,
}

impl<'a> Reply<'a> {
    /// Wraps an output buffer.
    pub fn new(buf: &'a mut Vec<u8>, resp3: bool) -> Self {
        Reply { buf, resp3 }
    }

    #[inline]
    fn header(&mut self, t: u8, n: i64) {
        self.buf.push(t);
        let mut b = itoa::Buffer::new();
        self.buf.extend_from_slice(b.format(n).as_bytes());
        self.buf.extend_from_slice(b"\r\n");
    }

    /// `+OK`.
    #[inline]
    pub fn ok(&mut self) {
        self.buf.extend_from_slice(b"+OK\r\n");
    }

    /// A simple string (`+...`).
    pub fn simple(&mut self, s: &str) {
        self.buf.push(b'+');
        self.buf.extend_from_slice(s.as_bytes());
        self.buf.extend_from_slice(b"\r\n");
    }

    /// An error (`-...`). Newlines are replaced so the reply stays one line.
    pub fn error(&mut self, s: &str) {
        self.buf.push(b'-');
        for c in s.bytes() {
            self.buf
                .push(if c == b'\r' || c == b'\n' { b' ' } else { c });
        }
        self.buf.extend_from_slice(b"\r\n");
    }

    /// An integer (`:n`).
    #[inline]
    pub fn int(&mut self, n: i64) {
        self.header(b':', n);
    }

    /// A bulk string.
    #[inline]
    pub fn bulk(&mut self, data: &[u8]) {
        self.header(b'$', data.len() as i64);
        self.buf.extend_from_slice(data);
        self.buf.extend_from_slice(b"\r\n");
    }

    /// A bulk string holding an integer's decimal form.
    pub fn bulk_int(&mut self, n: i64) {
        let mut b = itoa::Buffer::new();
        self.bulk(b.format(n).as_bytes());
    }

    /// Null bulk string (`$-1` / `_`).
    #[inline]
    pub fn null(&mut self) {
        if self.resp3 {
            self.buf.extend_from_slice(b"_\r\n");
        } else {
            self.buf.extend_from_slice(b"$-1\r\n");
        }
    }

    /// Null array (`*-1` / `_`).
    pub fn null_array(&mut self) {
        if self.resp3 {
            self.buf.extend_from_slice(b"_\r\n");
        } else {
            self.buf.extend_from_slice(b"*-1\r\n");
        }
    }

    /// Array header.
    #[inline]
    pub fn array(&mut self, n: usize) {
        self.header(b'*', n as i64);
    }

    /// Map header (`%n`, or an array of `2n` in RESP2).
    pub fn map(&mut self, n: usize) {
        if self.resp3 {
            self.header(b'%', n as i64);
        } else {
            self.header(b'*', 2 * n as i64);
        }
    }

    /// Set header (`~n`, or an array in RESP2).
    pub fn set(&mut self, n: usize) {
        if self.resp3 {
            self.header(b'~', n as i64);
        } else {
            self.header(b'*', n as i64);
        }
    }

    /// Push header (`>n`, or an array in RESP2).
    pub fn push(&mut self, n: usize) {
        if self.resp3 {
            self.header(b'>', n as i64);
        } else {
            self.header(b'*', n as i64);
        }
    }

    /// A double (`,1.5`, or a bulk string in RESP2).
    pub fn double(&mut self, v: f64) {
        let s = crate::util::fmt_f64(v);
        if self.resp3 {
            self.buf.push(b',');
            self.buf.extend_from_slice(s.as_bytes());
            self.buf.extend_from_slice(b"\r\n");
        } else {
            self.bulk(s.as_bytes());
        }
    }

    /// A boolean (`#t`/`#f`, or `:1`/`:0` in RESP2).
    pub fn boolean(&mut self, b: bool) {
        if self.resp3 {
            self.buf
                .extend_from_slice(if b { b"#t\r\n" } else { b"#f\r\n" });
        } else {
            self.int(b as i64);
        }
    }

    /// A verbatim string (`=len\r\ntxt:...`, or a bulk string in RESP2).
    pub fn verbatim(&mut self, text: &[u8]) {
        if self.resp3 {
            self.header(b'=', text.len() as i64 + 4);
            self.buf.extend_from_slice(b"txt:");
            self.buf.extend_from_slice(text);
            self.buf.extend_from_slice(b"\r\n");
        } else {
            self.bulk(text);
        }
    }

    /// Starts a reply whose element count is not known yet.
    pub fn begin_deferred(&mut self) -> usize {
        self.buf.len()
    }

    /// Finishes a deferred aggregate started at `at` with `n` elements, using
    /// `kind` (`b'*'`, `b'%'`, `b'~'`); maps count pairs.
    pub fn end_deferred(&mut self, at: usize, kind: u8, n: usize) {
        let (t, count) = match (kind, self.resp3) {
            (b'%', false) => (b'*', 2 * n),
            (b'~', false) => (b'*', n),
            (k, _) => (k, n),
        };
        let mut hdr = Vec::with_capacity(16);
        hdr.push(t);
        let mut b = itoa::Buffer::new();
        hdr.extend_from_slice(b.format(count).as_bytes());
        hdr.extend_from_slice(b"\r\n");
        self.buf.splice(at..at, hdr);
    }
}

/// Encodes a command as a RESP multibulk array (used for AOF and replication).
pub fn encode_command(out: &mut Vec<u8>, args: &[&[u8]]) {
    let mut b = itoa::Buffer::new();
    out.push(b'*');
    out.extend_from_slice(b.format(args.len()).as_bytes());
    out.extend_from_slice(b"\r\n");
    for a in args {
        out.push(b'$');
        out.extend_from_slice(b.format(a.len()).as_bytes());
        out.extend_from_slice(b"\r\n");
        out.extend_from_slice(a);
        out.extend_from_slice(b"\r\n");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_all(buf: &[u8]) -> Result<Vec<Vec<Vec<u8>>>, ProtocolError> {
        let mut p = Parser::new();
        let mut pos = 0;
        let mut out = Vec::new();
        while let Some(args) = p.parse(buf, &mut pos)? {
            out.push(match args {
                Args::Ranges(r) => r.iter().map(|&(s, l)| buf[s..s + l].to_vec()).collect(),
                Args::Owned(o) => o,
            });
        }
        Ok(out)
    }

    #[test]
    fn multibulk_and_pipelining() {
        let cmds = parse_all(b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n*1\r\n$4\r\nPING\r\n").unwrap();
        assert_eq!(
            cmds,
            vec![vec![b"GET".to_vec(), b"k".to_vec()], vec![b"PING".to_vec()]]
        );
        // Binary-safe payloads.
        let cmds = parse_all(b"*1\r\n$4\r\na\r\nb\r\n").unwrap();
        assert_eq!(cmds[0][0], b"a\r\nb");
    }

    #[test]
    fn incremental_byte_by_byte() {
        let input = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
        let mut p = Parser::new();
        let mut pos = 0;
        for end in 1..input.len() {
            assert!(
                p.parse(&input[..end], &mut pos).unwrap().is_none(),
                "complete at {end}"
            );
        }
        let args = p.parse(input, &mut pos).unwrap().unwrap();
        assert_eq!(pos, input.len());
        match args {
            Args::Ranges(r) => assert_eq!(r.len(), 3),
            _ => panic!(),
        }
    }

    #[test]
    fn inline_commands() {
        let cmds = parse_all(b"SET a \"hello world\"\r\nGET 'a'\nPING\r\n").unwrap();
        assert_eq!(
            cmds[0],
            vec![b"SET".to_vec(), b"a".to_vec(), b"hello world".to_vec()]
        );
        assert_eq!(cmds[1], vec![b"GET".to_vec(), b"a".to_vec()]);
        assert_eq!(cmds[2], vec![b"PING".to_vec()]);
        assert_eq!(split_args(b"\"\\x41\\n\"").unwrap(), vec![b"A\n".to_vec()]);
        assert!(split_args(b"\"unterminated").is_none());
        assert!(split_args(b"\"a\"b").is_none());
        assert_eq!(split_args(b"   ").unwrap(), Vec::<Vec<u8>>::new());
    }

    #[test]
    fn protocol_errors() {
        assert!(parse_all(b"*x\r\n").is_err());
        assert!(parse_all(b"*1\r\n+foo\r\n").is_err());
        assert!(parse_all(b"*1\r\n$-5\r\n").is_err());
        assert!(parse_all(&vec![b'a'; INLINE_MAX + 10]).is_err());
        assert_eq!(parse_all(b"*0\r\n").unwrap(), vec![Vec::<Vec<u8>>::new()]);
    }

    #[test]
    fn reply_encoding() {
        let mut buf = Vec::new();
        let mut r = Reply::new(&mut buf, false);
        r.map(1);
        r.bulk(b"k");
        r.double(1.5);
        r.null();
        assert_eq!(buf, b"*2\r\n$1\r\nk\r\n$3\r\n1.5\r\n$-1\r\n");
        let mut buf = Vec::new();
        let mut r = Reply::new(&mut buf, true);
        r.map(1);
        r.bulk(b"k");
        r.double(1.5);
        r.null();
        r.boolean(true);
        assert_eq!(buf, b"%1\r\n$1\r\nk\r\n,1.5\r\n_\r\n#t\r\n");
        let mut buf = b"prefix".to_vec();
        let mut r = Reply::new(&mut buf, false);
        let at = r.begin_deferred();
        r.bulk(b"a");
        r.end_deferred(at, b'*', 1);
        assert_eq!(buf, b"prefix*1\r\n$1\r\na\r\n");
    }
}
