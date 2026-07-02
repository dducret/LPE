use anyhow::{anyhow, Context, Result};
use std::{
    fs::File,
    io::{BufReader as StdBufReader, Cursor},
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context as TaskContext, Poll},
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::TcpStream,
};
use tokio_rustls::{
    rustls::{
        pki_types::{CertificateDer, PrivateKeyDer},
        ServerConfig,
    },
    TlsAcceptor,
};

pub(in crate::smtp) struct StartTlsStream {
    stream: TcpStream,
    prefix: Cursor<Vec<u8>>,
}

impl StartTlsStream {
    pub(in crate::smtp) fn new(stream: TcpStream, buffered: Vec<u8>) -> Self {
        Self {
            stream,
            prefix: Cursor::new(buffered),
        }
    }
}

impl AsyncRead for StartTlsStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let prefix_position = self.prefix.position() as usize;
        let prefix_len = self.prefix.get_ref().len();
        if prefix_position < prefix_len {
            let available = &self.prefix.get_ref()[prefix_position..];
            let to_copy = available.len().min(buf.remaining());
            buf.put_slice(&available[..to_copy]);
            self.prefix.set_position((prefix_position + to_copy) as u64);
            return Poll::Ready(Ok(()));
        }
        Pin::new(&mut self.stream).poll_read(cx, buf)
    }
}

impl AsyncWrite for StartTlsStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        data: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.stream).poll_write(cx, data)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.stream).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.stream).poll_shutdown(cx)
    }
}

pub(super) fn smtp_starttls_acceptor_from_store(
    dashboard_store: &Arc<Mutex<crate::DashboardState>>,
) -> Result<Option<TlsAcceptor>> {
    let (cert_path, key_path) = {
        let snapshot = dashboard_store
            .lock()
            .map_err(|_| anyhow!("dashboard state lock poisoned"))?;
        public_tls_paths_from_dashboard(&snapshot)
    };
    smtp_starttls_acceptor_for_paths(cert_path, key_path)
}

fn public_tls_paths_from_dashboard(
    dashboard: &crate::DashboardState,
) -> (Option<String>, Option<String>) {
    let Some(active_id) = dashboard
        .network
        .public_tls
        .active_profile_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return (None, None);
    };
    let Some(profile) = dashboard
        .network
        .public_tls
        .profiles
        .iter()
        .find(|profile| profile.id == active_id)
    else {
        return (None, None);
    };
    (
        Some(profile.cert_path.trim().to_string()).filter(|value| !value.is_empty()),
        Some(profile.key_path.trim().to_string()).filter(|value| !value.is_empty()),
    )
}

pub(crate) fn smtp_starttls_acceptor_for_paths(
    cert_path: Option<String>,
    key_path: Option<String>,
) -> Result<Option<TlsAcceptor>> {
    match (cert_path, key_path) {
        (Some(cert_path), Some(key_path)) => {
            let certificates = load_certificates(&cert_path)?;
            let key = load_private_key(&key_path)?;
            let config = ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(certificates, key)?;
            Ok(Some(TlsAcceptor::from(Arc::new(config))))
        }
        (None, None) => Ok(None),
        (Some(_), None) => Err(anyhow!(
            "LPE_CT_PUBLIC_TLS_KEY_PATH must be set when LPE_CT_PUBLIC_TLS_CERT_PATH is set"
        )),
        (None, Some(_)) => Err(anyhow!(
            "LPE_CT_PUBLIC_TLS_CERT_PATH must be set when LPE_CT_PUBLIC_TLS_KEY_PATH is set"
        )),
    }
}

fn load_certificates(path: &str) -> Result<Vec<CertificateDer<'static>>> {
    let mut reader = StdBufReader::new(
        File::open(path).with_context(|| format!("unable to open certificate {path}"))?,
    );
    rustls_pemfile::certs(&mut reader)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| anyhow!("unable to parse certificate {path}: {error}"))
        .and_then(|certificates| {
            if certificates.is_empty() {
                anyhow::bail!("no certificate found in {path}");
            }
            Ok(certificates)
        })
}

fn load_private_key(path: &str) -> Result<PrivateKeyDer<'static>> {
    let mut reader =
        StdBufReader::new(File::open(path).with_context(|| format!("unable to open key {path}"))?);
    let mut keys = rustls_pemfile::pkcs8_private_keys(&mut reader)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| anyhow!("unable to parse private key {path}: {error}"))?;
    if let Some(key) = keys.pop() {
        return Ok(PrivateKeyDer::Pkcs8(key));
    }

    let mut reader = StdBufReader::new(
        File::open(path).with_context(|| format!("unable to reopen key {path}"))?,
    );
    let mut keys = rustls_pemfile::rsa_private_keys(&mut reader)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| anyhow!("unable to parse rsa private key {path}: {error}"))?;
    let Some(key) = keys.pop() else {
        anyhow::bail!("no private key found in {path}");
    };
    Ok(PrivateKeyDer::Pkcs1(key))
}
