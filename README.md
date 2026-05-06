# Simple IRC Client - Network Module (Rust)

[![CI](https://github.com/Simple-Irc-Client/network-rs/actions/workflows/ci.yml/badge.svg)](https://github.com/Simple-Irc-Client/network-rs/actions/workflows/ci.yml)

A Rust IRC protocol client used by the desktop application. Embedded directly into the Tauri shell — there is no localhost WebSocket bridge.

## Features

- Async TCP and TLS connections to IRC servers
- PING/PONG keepalive with configurable PONG timeout
- CAP LS 302 negotiation with retries
- Full mode (auto NICK/USER registration) and raw mode
- Receive-buffer cap to defend against unterminated server lines
- Sliding-window rate limiter for outbound messages
- CR/LF stripping on outbound lines to prevent IRC line injection

## Requirements

- Rust >= 1.85 (stable)

## Getting Started

### Build

```bash
cargo build
```

### Test

```bash
cargo test
```

Integration tests in `tests/mock_server.rs` run against an in-process mock IRC server — no real network access required.

### Lint

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
```

## Related Projects

- [Simple-Irc-Client](https://github.com/Simple-Irc-Client) - Main project organization

## Contributing

If you find a bug or have a feature request, please [open an issue](https://github.com/Simple-Irc-Client/network-rs/issues) on GitHub.

## License

This project is licensed under the [GNU Affero General Public License v3.0 (AGPL-3.0)](https://github.com/Simple-Irc-Client/network-rs/blob/main/LICENSE).

The AGPL-3.0 license ensures that if you modify and deploy this software over a network, you must make the complete source code available to users.

**Authors:**

- [Piotr Łuczko](https://www.github.com/piotrluczko)
- [Dariusz Markowicz](https://www.github.com/dmarkowicz)
