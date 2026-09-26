//! An [`EnvelopeSigner`] at the other end of a socket: the engine holds no key, and whoever
//! does listens on a Unix socket, a TCP port, or any stream a connector opens (a vsock, say).
//!
//! The protocol is one connection per envelope and one line each way:
//!
//! ```text
//! engine → signer   the envelope as JSON, then '\n'
//! signer → engine   {"jws": "<compact JWS>"}  or  {"error": "<why>"}, then '\n'
//! ```

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::time::Duration;

use async_trait::async_trait;
use serde::Deserialize;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader};

use crate::envelope::{Envelope, EnvelopeSigner};

/// The longest answer read from a signer.
const MAX_ANSWER: u64 = 1 << 20;

trait Conn: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Conn for T {}

type Connect =
    Box<dyn Fn() -> Pin<Box<dyn Future<Output = io::Result<Box<dyn Conn>>> + Send>> + Send + Sync>;

pub struct SocketSigner {
    connect: Connect,
    timeout: Duration,
}

impl std::fmt::Debug for SocketSigner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SocketSigner")
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}

#[derive(Deserialize)]
struct Answer {
    jws: Option<String>,
    error: Option<String>,
}

impl SocketSigner {
    /// A signer reached through any stream `connect` opens, once per envelope.
    pub fn with_connector<F, Fut, S>(connect: F) -> SocketSigner
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = io::Result<S>> + Send + 'static,
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        SocketSigner {
            connect: Box::new(move || {
                let fut = connect();
                Box::pin(async move { fut.await.map(|s| Box::new(s) as Box<dyn Conn>) })
            }),
            timeout: Duration::from_secs(10),
        }
    }

    /// A signer listening on a Unix socket.
    #[cfg(unix)]
    pub fn unix(path: impl Into<std::path::PathBuf>) -> SocketSigner {
        let path = path.into();
        SocketSigner::with_connector(move || tokio::net::UnixStream::connect(path.clone()))
    }

    /// A signer listening on a TCP address (`host:port`).
    pub fn tcp(addr: impl Into<String>) -> SocketSigner {
        let addr = addr.into();
        SocketSigner::with_connector(move || tokio::net::TcpStream::connect(addr.clone()))
    }

    /// How long one envelope may take, connection included (default 10 s).
    pub fn with_timeout(mut self, timeout: Duration) -> SocketSigner {
        self.timeout = timeout;
        self
    }

    async fn exchange(&self, line: Vec<u8>) -> Result<String, String> {
        let mut conn = (self.connect)()
            .await
            .map_err(|e| format!("connect to the signer: {e}"))?;
        conn.write_all(&line)
            .await
            .map_err(|e| format!("send to the signer: {e}"))?;
        conn.flush()
            .await
            .map_err(|e| format!("send to the signer: {e}"))?;
        let mut answer = String::new();
        BufReader::new(conn.take(MAX_ANSWER))
            .read_line(&mut answer)
            .await
            .map_err(|e| format!("read from the signer: {e}"))?;
        if !answer.ends_with('\n') {
            return Err("the signer closed the connection without a whole answer".into());
        }
        let answer: Answer = serde_json::from_str(answer.trim_end())
            .map_err(|e| format!("the signer's answer is not JSON: {e}"))?;
        match (answer.jws, answer.error) {
            (_, Some(e)) => Err(format!("the signer refused: {e}")),
            (Some(jws), None) if !jws.is_empty() => Ok(jws),
            _ => Err("the signer answered with neither a signature nor an error".into()),
        }
    }
}

#[async_trait]
impl EnvelopeSigner for SocketSigner {
    async fn sign(&self, envelope: &Envelope) -> Result<String, String> {
        let mut line = serde_json::to_vec(envelope).map_err(|e| e.to_string())?;
        line.push(b'\n');
        tokio::time::timeout(self.timeout, self.exchange(line))
            .await
            .map_err(|_| format!("the signer did not answer within {:?}", self.timeout))?
    }
}
