//! Network transport for Simple Irc Client.
//!
//! [`IrcClient`] is a byte pipe to an IRC server: it connects over TCP or TLS, surfaces every received
//! line and writes the lines it is given. The app's kernel owns the IRC conversation itself.
//! [`dcc`] does the same for DCC CHAT and DCC SEND peers.

mod client;
mod codec;
pub mod dcc;
pub mod error;
mod ratelimit;

pub use client::{IrcClient, IrcClientOptions, IrcEvent};
pub use codec::Encoding;
pub use dcc::{DccConnectOptions, DccError, DccEvent, DccListenOptions, DccSession};
pub use error::IrcError;
