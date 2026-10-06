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
    /// Binds the first free port in `[start, end]`, or any free port when both are 0.
    ///
    /// The range is for users who forwarded specific ports on their router.
    pub fn bind(start: u16, end: u16) -> Result<Self, DccError> {
        if start == 0 && end == 0 {
            return Self::from_std(std::net::TcpListener::bind((Ipv4Addr::UNSPECIFIED, 0))?);
        }

        let (low, high) = if start <= end {
            (start, end)
        } else {
            (end, start)
        };
        (low..=high)
            .find_map(|port| std::net::TcpListener::bind((Ipv4Addr::UNSPECIFIED, port)).ok())
            .map_or(Err(DccError::NoFreePort), Self::from_std)
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

    /// Accepts one connection, from `expect_peer` if given.
    ///
    /// The offer travels over IRC, so others can see the port. Connections from other addresses are
    /// dropped and waiting continues, so a prober can neither take the session nor use up the slot.
    pub async fn accept_from(
        &self,
        expect_peer: Option<IpAddr>,
        timeout: Duration,
    ) -> Result<TcpStream, DccError> {
        let deadline = time::Instant::now() + timeout;

        loop {
            let (socket, peer) = time::timeout_at(deadline, self.inner.accept())
                .await
                .map_err(|_| DccError::Timeout)??;

            match expect_peer {
                Some(expected) if !same_host(expected, peer) => continue,
                _ => return Ok(socket),
            }
        }
    }
}

/// An IPv4-mapped IPv6 address (`::ffff:1.2.3.4`) is the same host as its IPv4 form.
fn same_host(expected: IpAddr, actual: SocketAddr) -> bool {
    expected.to_canonical() == actual.ip().to_canonical()
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

    // `bind` hands the socket to tokio, so these need a reactor

    #[tokio::test]
    async fn bind_any_gets_a_real_port() {
        let listener = DccListener::bind(0, 0).unwrap();
        assert!(listener.port() > 0);
    }

    #[tokio::test]
    async fn bind_range_honours_the_range() {
        let listener = DccListener::bind(0, 0).unwrap();
        let port = listener.port();
        // A one-port range covering the port we already hold has nothing free
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
