---
type: Rust Module
title: service
resource: crates/lpe-managesieve/src/service.rs#L1-L227
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-anyhow-bail-result
  - external/lpe-storage-auditentryinput
  - external/tokio-io-asyncwriteext-bufreader-net-tcplistener-tcpstream
  - external/crate-auth-authenticate-require-auth-parse-as-string-read-request-single-string-arg-argument-store-managesievestore
  member_of:
  - packages/crates/lpe-managesieve
---

# Contains

- [ManageSieveServer](../../../../classes/crates/lpe-managesieve/src/service/ManageSieveServer.md)
- [new](../../../../functions/crates/lpe-managesieve/src/service/ManageSieveServer/new.md)
- [serve](../../../../functions/crates/lpe-managesieve/src/service/ManageSieveServer/serve.md)
- [serve](../../../../functions/crates/lpe-managesieve/src/service/serve.md)
- [handle_connection](../../../../functions/crates/lpe-managesieve/src/service/handle_connection.md)
- [write_capability](../../../../functions/crates/lpe-managesieve/src/service/write_capability.md)
- [handle_havespace](../../../../functions/crates/lpe-managesieve/src/service/handle_havespace.md)

# Imports

- `anyhow::{anyhow, bail, Result}`
- `lpe_storage::AuditEntryInput`
- `tokio::{
    io::{AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
}`
- `crate::{
    auth::{authenticate, require_auth},
    parse::{as_string, read_request, single_string_arg, Argument},
    store::ManageSieveStore,
}`

# Member of

- [lpe-managesieve](../../../../packages/crates/lpe-managesieve.md)