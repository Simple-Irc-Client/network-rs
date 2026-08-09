//! One-shot TCP listener for the offering side of a DCC session.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

use tokio::net::{TcpListener, TcpStream};
use tokio::time;

use super::DccError;

pub struct DccListener {
    inner: TcpListener,
    port: u16,
}

impl DccListener {
    /// Bind inside `[start, end]`, or on any free port when both are 0.
    ///
    /// A range exists for users who forwarded specific ports on their router;
    /// everyone else is better served by an ephemeral port.
    pub fn bind(start: u16, end: u16) -> Result<Self, DccError> {
        if start == 0 && end == 0 {
            let inner = std::net::TcpListener::bind((Ipv4Addr::UNSPECIFIED, 0))?;
            return Self::from_std(inner);
        }

        let (low, high) = if start <= end {
            (start, end)
        } else {
            (end, start)
        };
        for port in low..=high {
            if let Ok(inner) = std::net::TcpListener::bind((Ipv4Addr::UNSPECIFIED, port)) {
                return Self::from_std(inner);
            }
        }
        Err(DccError::NoFreePort)
    }

    fn from_std(inner: std::net::TcpListener) -> Result<Self, DccError> {
        inner.set_nonblocking(true)?;
        let port = inner.local_addr()?.port();
        Ok(Self {
            inner: TcpListener::from_std(inner)?,
            port,
        })
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    /// Accept exactly one connection, from `expect_peer` if given.
    ///
    /// The address check matters: we advertised this port over IRC, where the
    /// offer is visible to the server (and to anyone the peer forwards it to).
    /// Without the check, whoever reaches the port first wins the session.
    /// Connections from other addresses are dropped and we keep waiting, so a
    /// prober cannot consume the slot the real peer needs.
    pub async fn accept_from(
        &self,
        expect_peer: Option<IpAddr>,
        timeout: Duration,
    ) -> Result<TcpStream, DccError> {
        let deadline = time::Instant::now() + timeout;

        loop {
            let remaining = deadline.saturating_duration_since(time::Instant::now());
            if remaining.is_zero() {
                return Err(DccError::Timeout);
            }

            let (socket, peer) = time::timeout(remaining, self.inner.accept())
                .await
                .map_err(|_| DccError::Timeout)??;

            match expect_peer {
                Some(expected) if !same_host(expected, peer) => {
                    drop(socket);
                    continue;
                }
                _ => return Ok(socket),
            }
        }
    }
}

/// Compare the expected peer with the connecting address, treating an
/// IPv4-mapped IPv6 address as equal to its IPv4 form — a dual-stack listener
/// reports `::ffff:1.2.3.4` for a peer that offered `1.2.3.4`.
fn same_host(expected: IpAddr, actual: SocketAddr) -> bool {
    let actual = match actual.ip() {
        IpAddr::V6(v6) => v6
            .to_ipv4_mapped()
            .map(IpAddr::V4)
            .unwrap_or(IpAddr::V6(v6)),
        ip => ip,
    };
    let expected = match expected {
        IpAddr::V6(v6) => v6
            .to_ipv4_mapped()
            .map(IpAddr::V4)
            .unwrap_or(IpAddr::V6(v6)),
        ip => ip,
    };
    expected == actual
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ipv4_mapped_peer_matches_its_ipv4_form() {
        let expected: IpAddr = "1.2.3.4".parse().unwrap();
        let mapped: SocketAddr = "[::ffff:1.2.3.4]:1234".parse().unwrap();
        assert!(same_host(expected, mapped));
    }

    #[test]
    fn different_hosts_do_not_match() {
        let expected: IpAddr = "1.2.3.4".parse().unwrap();
        let other: SocketAddr = "5.6.7.8:1234".parse().unwrap();
        assert!(!same_host(expected, other));
    }

    // `bind` hands the socket to tokio, so these need a reactor.

    #[tokio::test]
    async fn bind_any_gets_a_real_port() {
        let listener = DccListener::bind(0, 0).unwrap();
        assert!(listener.port() > 0);
    }

    #[tokio::test]
    async fn bind_range_honours_the_range() {
        let listener = DccListener::bind(0, 0).unwrap();
        let port = listener.port();
        // A one-port range covering the port we already hold has nothing free.
        assert!(matches!(
            DccListener::bind(port, port),
            Err(DccError::NoFreePort)
        ));
    }

    #[tokio::test]
    async fn accept_times_out_when_nobody_connects() {
        let listener = DccListener::bind(0, 0).unwrap();
        let result = listener.accept_from(None, Duration::from_millis(50)).await;
        assert!(matches!(result, Err(DccError::Timeout)));
    }
}
