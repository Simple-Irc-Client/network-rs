//! IRC byte-pipe transport for Simple Irc Client.
//!
//! Async [`IrcClient`] that establishes a TCP or TLS connection to an IRC
//! server and surfaces every received line over an [`IrcEvent`] channel, while
//! writing the lines the caller sends. It is a pure transport: it speaks no IRC
//! protocol itself (no registration, CAP, or PING/PONG) — the caller owns the
//! entire IRC conversation.

mod client;
mod codec;
pub mod error;
mod ratelimit;

pub use client::{Encoding, IrcClient, IrcClientOptions, IrcEvent};
pub use error::IrcError;
pub use ratelimit::SlidingWindowLimiter;
