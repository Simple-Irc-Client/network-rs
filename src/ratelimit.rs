//! Sliding-window rate limiter.
//!
//! Free-standing so the call site (Tauri command layer, etc.) can apply it
//! on the inbound side without coupling the IRC client to a rate-limit policy.
//! Default policy ([`SlidingWindowLimiter::default_irc`]) is 50 msgs / 5 s.

use std::time::{Duration, Instant};

pub struct SlidingWindowLimiter {
    max_messages: u32,
    window: Duration,
    count: u32,
    window_start: Instant,
}

impl SlidingWindowLimiter {
    pub fn new(max_messages: u32, window: Duration) -> Self {
        Self {
            max_messages,
            window,
            count: 0,
            window_start: Instant::now(),
        }
    }

    /// Default IRC backend policy: 50 messages / 5 seconds.
    pub fn default_irc() -> Self {
        Self::new(50, Duration::from_secs(5))
    }

    /// Returns `true` if the message is allowed and consumes a slot.
    /// Returns `false` if the window is exhausted; the caller should drop
    /// the message.
    pub fn check_and_consume(&mut self) -> bool {
        let now = Instant::now();
        if now.duration_since(self.window_start) > self.window {
            self.count = 0;
            self.window_start = now;
        }
        self.count += 1;
        self.count <= self.max_messages
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allows_up_to_max_then_drops() {
        let mut lim = SlidingWindowLimiter::new(3, Duration::from_secs(1));
        assert!(lim.check_and_consume());
        assert!(lim.check_and_consume());
        assert!(lim.check_and_consume());
        assert!(!lim.check_and_consume());
        assert!(!lim.check_and_consume());
    }

    #[test]
    fn resets_after_window() {
        let mut lim = SlidingWindowLimiter::new(1, Duration::from_millis(50));
        assert!(lim.check_and_consume());
        assert!(!lim.check_and_consume());
        std::thread::sleep(Duration::from_millis(80));
        assert!(lim.check_and_consume());
    }
}
