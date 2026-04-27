use anyhow::{anyhow, bail, Context, Result};
use std::{env, fs::File, io::BufReader, net::SocketAddr, path::PathBuf, sync::Arc};
use tokio::{
    io::{copy_bidirectional, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use tokio_rustls::{
    rustls::{pki_types::CertificateDer, pki_types::PrivateKeyDer, ServerConfig},
    TlsAcceptor,
};
use tracing::{info, warn};

pub(crate) async fn run_imaps_proxy(
    bind_address: String,
    upstream_address: String,
    cert_path: PathBuf,
    key_path: PathBuf,
) -> Result<()> {
    let tls = load_tls_acceptor(&cert_path, &key_path)?;
    let listener = TcpListener::bind(&bind_address)
        .await
        .with_context(|| format!("unable to bind IMAPS proxy on {bind_address}"))?;
    info!(
        upstream = %upstream_address,
        "lpe-ct imaps proxy active on {bind_address}"
    );

    loop {
        let (stream, peer) = listener.accept().await?;
        let tls = tls.clone();
        let upstream_address = upstream_address.clone();
        tokio::spawn(async move {
            if let Err(error) = handle_imaps_session(stream, peer, tls, upstream_address).await {
                warn!(peer = %peer, error = %error, "imaps proxy session failed");
            }
        });
    }
}

async fn handle_imaps_session(
    stream: TcpStream,
    _peer: SocketAddr,
    tls: TlsAcceptor,
    upstream_address: String,
) -> Result<()> {
    if let Some(role) = crate::ha_non_active_role_for_traffic()? {
        let mut stream = stream;
        stream
            .write_all(
                format!("* BYE node role {role} is not accepting IMAPS traffic\r\n").as_bytes(),
            )
            .await?;
        return Ok(());
    }

    let mut client = tls.accept(stream).await?;
    let mut upstream = TcpStream::connect(&upstream_address)
        .await
        .with_context(|| format!("unable to connect to LPE IMAP upstream {upstream_address}"))?;
    copy_bidirectional(&mut client, &mut upstream).await?;
    Ok(())
}

fn load_tls_acceptor(cert_path: &PathBuf, key_path: &PathBuf) -> Result<TlsAcceptor> {
    let certificates = load_certificates(cert_path)?;
    let key = load_private_key(key_path)?;
    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certificates, key)?;
    Ok(TlsAcceptor::from(Arc::new(config)))
}

fn load_certificates(path: &PathBuf) -> Result<Vec<CertificateDer<'static>>> {
    let mut reader = BufReader::new(
        File::open(path)
            .with_context(|| format!("unable to open certificate {}", path.display()))?,
    );
    rustls_pemfile::certs(&mut reader)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| anyhow!("unable to parse certificate {}: {error}", path.display()))
}

fn load_private_key(path: &PathBuf) -> Result<PrivateKeyDer<'static>> {
    let mut reader = BufReader::new(
        File::open(path).with_context(|| format!("unable to open key {}", path.display()))?,
    );
    let mut keys = rustls_pemfile::pkcs8_private_keys(&mut reader)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| anyhow!("unable to parse private key {}: {error}", path.display()))?;
    if let Some(key) = keys.pop() {
        return Ok(PrivateKeyDer::Pkcs8(key));
    }

    let mut reader = BufReader::new(
        File::open(path).with_context(|| format!("unable to reopen key {}", path.display()))?,
    );
    let mut keys = rustls_pemfile::rsa_private_keys(&mut reader)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| {
            anyhow!(
                "unable to parse rsa private key {}: {error}",
                path.display()
            )
        })?;
    let Some(key) = keys.pop() else {
        bail!("no private key found in {}", path.display());
    };
    Ok(PrivateKeyDer::Pkcs1(key))
}

pub(crate) fn imaps_bind_address() -> Option<String> {
    non_empty_env("LPE_CT_IMAPS_BIND_ADDRESS")
}

pub(crate) fn imaps_upstream_address() -> String {
    non_empty_env("LPE_CT_IMAPS_UPSTREAM_ADDRESS").unwrap_or_else(|| "127.0.0.1:1143".to_string())
}

pub(crate) fn imaps_tls_cert_path() -> Option<PathBuf> {
    non_empty_env("LPE_CT_IMAPS_TLS_CERT_PATH")
        .or_else(|| non_empty_env("LPE_CT_PUBLIC_TLS_CERT_PATH"))
        .map(PathBuf::from)
}

pub(crate) fn imaps_tls_key_path() -> Option<PathBuf> {
    non_empty_env("LPE_CT_IMAPS_TLS_KEY_PATH")
        .or_else(|| non_empty_env("LPE_CT_PUBLIC_TLS_KEY_PATH"))
        .map(PathBuf::from)
}

fn non_empty_env(name: &str) -> Option<String> {
    env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}
