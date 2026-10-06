//! Newline framing and text encoding shared by IRC and DCC CHAT.

/// A peer that never terminates a line must not grow the buffer without bound.
pub const MAX_RECEIVE_BUFFER: usize = 2 * 1024 * 1024;

#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub enum Encoding {
    #[default]
    Utf8,
    Latin1,
}

impl Encoding {
    fn decode(self, bytes: &[u8]) -> String {
        match self {
            Encoding::Utf8 => String::from_utf8_lossy(bytes).into_owned(),
            // Every byte is the Unicode code point of the same value
            Encoding::Latin1 => bytes.iter().map(|&b| char::from(b)).collect(),
        }
    }
}

#[derive(Default)]
pub struct LineBuffer {
    pending: Vec<u8>,
}

impl LineBuffer {
    pub fn extend(&mut self, data: &[u8]) {
        self.pending.extend_from_slice(data);
    }

    /// Bytes of the unterminated line still waiting for its newline.
    pub fn len(&self) -> usize {
        self.pending.len()
    }

    /// Next complete non-empty line, without its terminator.
    ///
    /// RFC 1459 mandates CRLF, but some servers and bouncers send a bare LF, so both are accepted.
    pub fn next_line(&mut self, encoding: Encoding) -> Option<String> {
        loop {
            let newline = self.pending.iter().position(|&b| b == b'\n')?;
            let mut line: Vec<u8> = self.pending.drain(..=newline).collect();
            line.pop();
            if line.last() == Some(&b'\r') {
                line.pop();
            }
            if !line.is_empty() {
                return Some(encoding.decode(&line));
            }
        }
    }
}

/// Removes CR and LF, so one outgoing line can never become two (line injection).
pub fn strip_crlf(s: &str) -> String {
    s.chars().filter(|&c| c != '\r' && c != '\n').collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_on_crlf() {
        let mut buf = LineBuffer::default();
        buf.extend(b"PING :foo\r\nPONG :bar\r\n");
        assert_eq!(buf.next_line(Encoding::Utf8).as_deref(), Some("PING :foo"));
        assert_eq!(buf.next_line(Encoding::Utf8).as_deref(), Some("PONG :bar"));
        assert_eq!(buf.next_line(Encoding::Utf8), None);
    }

    #[test]
    fn drops_empty_lines() {
        let mut buf = LineBuffer::default();
        buf.extend(b"\r\n\r\nPING\r\n");
        assert_eq!(buf.next_line(Encoding::Utf8).as_deref(), Some("PING"));
        assert_eq!(buf.next_line(Encoding::Utf8), None);
    }

    #[test]
    fn splits_on_bare_lf() {
        let mut buf = LineBuffer::default();
        buf.extend(b"PING :foo\nPONG :bar\n");
        assert_eq!(buf.next_line(Encoding::Utf8).as_deref(), Some("PING :foo"));
        assert_eq!(buf.next_line(Encoding::Utf8).as_deref(), Some("PONG :bar"));
        assert_eq!(buf.next_line(Encoding::Utf8), None);
    }

    #[test]
    fn handles_mixed_crlf_and_lf() {
        let mut buf = LineBuffer::default();
        buf.extend(b"A :crlf\r\nB :lf\nC :crlf\r\n");
        assert_eq!(buf.next_line(Encoding::Utf8).as_deref(), Some("A :crlf"));
        assert_eq!(buf.next_line(Encoding::Utf8).as_deref(), Some("B :lf"));
        assert_eq!(buf.next_line(Encoding::Utf8).as_deref(), Some("C :crlf"));
        assert_eq!(buf.next_line(Encoding::Utf8), None);
    }

    #[test]
    fn handles_partial_lines() {
        let mut buf = LineBuffer::default();
        buf.extend(b"PING :fo");
        assert!(buf.next_line(Encoding::Utf8).is_none());
        buf.extend(b"o\r\n");
        assert_eq!(buf.next_line(Encoding::Utf8).as_deref(), Some("PING :foo"));
    }

    #[test]
    fn len_counts_only_the_unterminated_tail() {
        let mut buf = LineBuffer::default();
        buf.extend(b"PING\r\nPAR");
        while buf.next_line(Encoding::Utf8).is_some() {}
        assert_eq!(buf.len(), 3);
    }

    #[test]
    fn decodes_latin1_bytes_as_code_points() {
        let mut buf = LineBuffer::default();
        buf.extend(b"caf\xe9\r\n");
        assert_eq!(buf.next_line(Encoding::Latin1).as_deref(), Some("café"));
    }

    #[test]
    fn strip_crlf_removes_both() {
        assert_eq!(strip_crlf("a\r\nb\nc\rd"), "abcd");
    }
}
