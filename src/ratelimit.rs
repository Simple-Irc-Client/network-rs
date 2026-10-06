use std::time::{Duration, Instant};

/// Allows `max_messages` per fixed window; the count resets when a window has elapsed.
pub struct RateLimiter {
    max_messages: u32,
    window: Duration,
    count: u32,
    window_start: Instant,
}

impl RateLimiter {
    pub fn new(max_messages: u32, window: Duration) -> Self {
        Self {
            max_messages,
            window,
            count: 0,
            window_start: Instant::now(),
        }
    }

    /// Consumes a slot; `false` means the window is exhausted and the message should be dropped.
    pub fn try_acquire(&mut self) -> bool {
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
        let mut limiter = RateLimiter::new(3, Duration::from_secs(1));
        assert!(limiter.try_acquire());
        assert!(limiter.try_acquire());
        assert!(limiter.try_acquire());
        assert!(!limiter.try_acquire());
        assert!(!limiter.try_acquire());
    }

    #[test]
    fn resets_after_window() {
        let mut limiter = RateLimiter::new(1, Duration::from_millis(50));
        assert!(limiter.try_acquire());
        assert!(!limiter.try_acquire());
        std::thread::sleep(Duration::from_millis(80));
        assert!(limiter.try_acquire());
    }
}
