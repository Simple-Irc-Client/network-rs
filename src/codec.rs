//! Line-oriented codec for IRC.
//!
//! IRC is a CRLF-terminated text protocol over a byte stream. The 2 MiB cap
//! defends against a hostile or buggy server that never terminates a line —
//! without it the buffer grows unbounded.

use crate::client::Encoding;

pub const MAX_RECEIVE_BUFFER: usize = 2 * 1024 * 1024;

pub struct LineBuffer {
    inner: Vec<u8>,
}

impl LineBuffer {
    pub fn new() -> Self {
        Self {
            inner: Vec::with_capacity(4096),
        }
    }

    pub fn extend(&mut self, data: &[u8]) {
        self.inner.extend_from_slice(data);
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Pop the next complete line (without the terminator) if one is buffered.
    /// Empty lines are skipped.
    ///
    /// RFC 1459/2812 mandates `\r\n`, but some bouncers, proxies and test
    /// servers emit a bare `\n`. Splitting strictly on `\r\n` would let such
    /// a stream grow until it trips `MAX_RECEIVE_BUFFER` and the connection
    /// is killed. So split on `\n` and strip an optional trailing `\r`,
    /// accepting `\r\n`, `\n`, and even a stray lone `\r` inside a line.
    pub fn next_line(&mut self, encoding: Encoding) -> Option<String> {
        loop {
            let idx = self.inner.iter().position(|&b| b == b'\n')?;
            let mut line_bytes: Vec<u8> = self.inner.drain(..idx).collect();
            // Drop the '\n'.
            self.inner.drain(..1);
            // Strip the '\r' of a '\r\n' terminator if present.
            if line_bytes.last() == Some(&b'\r') {
                line_bytes.pop();
            }
            if !line_bytes.is_empty() {
                return Some(decode(encoding, &line_bytes));
            }
        }
    }
}

fn decode(encoding: Encoding, bytes: &[u8]) -> String {
    match encoding {
        Encoding::Utf8 => String::from_utf8_lossy(bytes).into_owned(),
        // Latin-1 maps each byte 1:1 to a codepoint <= 0xFF.
        Encoding::Latin1 => bytes.iter().map(|&b| b as char).collect(),
    }
}

/// Strip CR/LF from outgoing lines to prevent IRC line injection.
pub fn strip_crlf(s: &str) -> String {
    s.chars().filter(|&c| c != '\r' && c != '\n').collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_on_crlf() {
        let mut buf = LineBuffer::new();
        buf.extend(b"PING :foo\r\nPONG :bar\r\n");
        assert_eq!(buf.next_line(Encoding::Utf8).as_deref(), Some("PING :foo"));
        assert_eq!(buf.next_line(Encoding::Utf8).as_deref(), Some("PONG :bar"));
        assert_eq!(buf.next_line(Encoding::Utf8), None);
    }

    #[test]
    fn drops_empty_lines() {
        let mut buf = LineBuffer::new();
        buf.extend(b"\r\n\r\nPING\r\n");
        assert_eq!(buf.next_line(Encoding::Utf8).as_deref(), Some("PING"));
        assert_eq!(buf.next_line(Encoding::Utf8), None);
    }

    #[test]
    fn splits_on_bare_lf() {
        let mut buf = LineBuffer::new();
        buf.extend(b"PING :foo\nPONG :bar\n");
        assert_eq!(buf.next_line(Encoding::Utf8).as_deref(), Some("PING :foo"));
        assert_eq!(buf.next_line(Encoding::Utf8).as_deref(), Some("PONG :bar"));
        assert_eq!(buf.next_line(Encoding::Utf8), None);
    }

    #[test]
    fn handles_mixed_crlf_and_lf() {
        let mut buf = LineBuffer::new();
        buf.extend(b"A :crlf\r\nB :lf\nC :crlf\r\n");
        assert_eq!(buf.next_line(Encoding::Utf8).as_deref(), Some("A :crlf"));
        assert_eq!(buf.next_line(Encoding::Utf8).as_deref(), Some("B :lf"));
        assert_eq!(buf.next_line(Encoding::Utf8).as_deref(), Some("C :crlf"));
        assert_eq!(buf.next_line(Encoding::Utf8), None);
    }

    #[test]
    fn handles_partial_lines() {
        let mut buf = LineBuffer::new();
        buf.extend(b"PING :fo");
        assert!(buf.next_line(Encoding::Utf8).is_none());
        buf.extend(b"o\r\n");
        assert_eq!(buf.next_line(Encoding::Utf8).as_deref(), Some("PING :foo"));
    }

    #[test]
    fn strip_crlf_removes_both() {
        assert_eq!(strip_crlf("a\r\nb\nc\rd"), "abcd");
    }
}
