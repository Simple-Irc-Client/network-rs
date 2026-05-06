//! IRC protocol client for Simple Irc Client.
//!
//! Async [`IrcClient`] that establishes a TCP or TLS connection to an IRC
//! server, performs PING/PONG keepalive, drives CAP LS negotiation, and
//! surfaces every line over an [`IrcEvent`] channel so a higher layer can
//! forward messages however it likes.

mod client;
mod codec;
pub mod error;
mod ratelimit;

pub use client::{Encoding, IrcClient, IrcClientOptions, IrcEvent, RegistrationOptions};
pub use error::IrcError;
pub use ratelimit::SlidingWindowLimiter;
