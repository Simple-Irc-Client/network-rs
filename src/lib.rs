//! IRC byte-pipe transport for Simple Irc Client.
//!
//! [`IrcClient`] connects to an IRC server over TCP or TLS, surfaces every received line and writes
//! the lines it is given. The app's kernel owns the IRC conversation itself.

mod client;
mod codec;
pub mod error;
mod ratelimit;

pub use client::{IrcClient, IrcClientOptions, IrcEvent};
pub use codec::Encoding;
pub use error::IrcError;
