---
type: Rust Module
title: tls
resource: LPE-CT/src/smtp/tls.rs#L1-L173
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-anyhow-context-result
  - external/std-fs-file-io-bufreader-as-stdbufreader-cursor-pin-pin-sync-arc-mutex-task-context-as-taskcontext-poll
  - external/tokio-io-asyncread-asyncwrite-readbuf-net-tcpstream
  - external/tokio-rustls-rustls-pki-types-certificateder-privatekeyder-serverconfig-tlsacceptor
  member_of:
  - packages/LPE-CT
---

# Contains

- [StartTlsStream](../../../../classes/LPE-CT/src/smtp/tls/StartTlsStream.md)
- [new](../../../../functions/LPE-CT/src/smtp/tls/StartTlsStream/new.md)
- [poll_read](../../../../functions/LPE-CT/src/smtp/tls/StartTlsStream/asyncread/poll_read.md)
- [poll_write](../../../../functions/LPE-CT/src/smtp/tls/StartTlsStream/asyncwrite/poll_write.md)
- [poll_flush](../../../../functions/LPE-CT/src/smtp/tls/StartTlsStream/asyncwrite/poll_flush.md)
- [poll_shutdown](../../../../functions/LPE-CT/src/smtp/tls/StartTlsStream/asyncwrite/poll_shutdown.md)
- [smtp_starttls_acceptor_from_store](../../../../functions/LPE-CT/src/smtp/tls/smtp_starttls_acceptor_from_store.md)
- [public_tls_paths_from_dashboard](../../../../functions/LPE-CT/src/smtp/tls/public_tls_paths_from_dashboard.md)
- [smtp_starttls_acceptor_for_paths](../../../../functions/LPE-CT/src/smtp/tls/smtp_starttls_acceptor_for_paths.md)
- [load_certificates](../../../../functions/LPE-CT/src/smtp/tls/load_certificates.md)
- [load_private_key](../../../../functions/LPE-CT/src/smtp/tls/load_private_key.md)

# Imports

- `anyhow::{anyhow, Context, Result}`
- `std::{
    fs::File,
    io::{BufReader as StdBufReader, Cursor},
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context as TaskContext, Poll},
}`
- `tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::TcpStream,
}`
- `tokio_rustls::{
    rustls::{
        pki_types::{CertificateDer, PrivateKeyDer},
        ServerConfig,
    },
    TlsAcceptor,
}`

# Member of

- [lpe-ct](../../../../packages/LPE-CT.md)