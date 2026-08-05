---
type: Rust Module
title: service
resource: crates/lpe-imap/src/service.rs#L1-L623
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-anyhow-bail-result
  - external/lpe-magika-detector-validator
  - external/lpe-mail-auth-accountprincipal
  - external/lpe-storage-imapemail
  - external/std-str-sync-arc
  - external/tokio-io-asyncbufreadext-asyncreadext-asyncwriteext-bufreader-net-tcplistener-tcpstream
  - external/tracing-warn
  - external/uuid-uuid
  - external/crate-mailboxes-render-mailbox-path-parse-parse-request-line-render-render-imap-mailbox-response-path-render-selected-updates-sanitize-imap-text-store-imapstore
  member_of:
  - packages/crates/lpe-imap
---

# Contains

- [ImapServer](../../../../classes/crates/lpe-imap/src/service/ImapServer.md)
- [new](../../../../functions/crates/lpe-imap/src/service/ImapServer/new.md)
- [with_validator](../../../../functions/crates/lpe-imap/src/service/ImapServer/with_validator.md)
- [serve](../../../../functions/crates/lpe-imap/src/service/ImapServer/serve.md)
- [handle_connection](../../../../functions/crates/lpe-imap/src/service/ImapServer/handle_connection.md)
- [read_command_literals](../../../../functions/crates/lpe-imap/src/service/read_command_literals.md)
- [decode_command_line](../../../../functions/crates/lpe-imap/src/service/decode_command_line.md)
- [trim_line_end](../../../../functions/crates/lpe-imap/src/service/trim_line_end.md)
- [command_tag_from_bytes](../../../../functions/crates/lpe-imap/src/service/command_tag_from_bytes.md)
- [trailing_literal](../../../../functions/crates/lpe-imap/src/service/trailing_literal.md)
- [quote_literal_token](../../../../functions/crates/lpe-imap/src/service/quote_literal_token.md)
- [serve](../../../../functions/crates/lpe-imap/src/service/serve.md)
- [Session](../../../../classes/crates/lpe-imap/src/service/Session.md)
- [SelectedMailbox](../../../../classes/crates/lpe-imap/src/service/SelectedMailbox.md)
- [MessageRefKind](../../../../classes/crates/lpe-imap/src/service/MessageRefKind.md)
- [new](../../../../functions/crates/lpe-imap/src/service/Session/new.md)
- [handle_request](../../../../functions/crates/lpe-imap/src/service/Session/handle_request.md)
- [handle_capability](../../../../functions/crates/lpe-imap/src/service/Session/handle_capability.md)
- [handle_noop](../../../../functions/crates/lpe-imap/src/service/Session/handle_noop.md)
- [refresh_selected_updates](../../../../functions/crates/lpe-imap/src/service/Session/refresh_selected_updates.md)
- [handle_id](../../../../functions/crates/lpe-imap/src/service/Session/handle_id.md)
- [handle_enable](../../../../functions/crates/lpe-imap/src/service/Session/handle_enable.md)
- [handle_getquotaroot](../../../../functions/crates/lpe-imap/src/service/Session/handle_getquotaroot.md)
- [handle_getquota](../../../../functions/crates/lpe-imap/src/service/Session/handle_getquota.md)
- [handle_logout](../../../../functions/crates/lpe-imap/src/service/Session/handle_logout.md)
- [require_auth](../../../../functions/crates/lpe-imap/src/service/Session/require_auth.md)
- [require_selected](../../../../functions/crates/lpe-imap/src/service/Session/require_selected.md)
- [refresh_selected](../../../../functions/crates/lpe-imap/src/service/Session/refresh_selected.md)

# Imports

- `anyhow::{anyhow, bail, Result}`
- `lpe_magika::{Detector, Validator}`
- `lpe_mail_auth::AccountPrincipal`
- `lpe_storage::ImapEmail`
- `std::{str, sync::Arc}`
- `tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
}`
- `tracing::warn`
- `uuid::Uuid`
- `crate::{
    mailboxes::render_mailbox_path,
    parse::parse_request_line,
    render::{render_imap_mailbox_response_path, render_selected_updates, sanitize_imap_text},
    store::ImapStore,
}`

# Member of

- [lpe-imap](../../../../packages/crates/lpe-imap.md)