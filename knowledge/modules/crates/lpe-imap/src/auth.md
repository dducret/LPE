---
type: Rust Module
title: auth
resource: crates/lpe-imap/src/auth.rs#L1-L222
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-anyhow-bail-result
  - external/base64-engine-general-purpose-standard-as-base64-engine-as
  - external/lpe-magika-detector
  - external/lpe-mail-auth-authenticate-bearer-access-token-authenticate-plain-credentials
  - external/tokio-io-asyncbufreadext-asyncreadext-asyncwriteext-bufreader
  - external/crate-parse-tokenize-session
  member_of:
  - packages/crates/lpe-imap
---

# Contains

- [handle_login](../../../../functions/crates/lpe-imap/src/auth/Session/handle_login.md)
- [handle_authenticate](../../../../functions/crates/lpe-imap/src/auth/Session/handle_authenticate.md)
- [read_authenticate_response](../../../../functions/crates/lpe-imap/src/auth/read_authenticate_response.md)
- [parse_plain_initial_response](../../../../functions/crates/lpe-imap/src/auth/parse_plain_initial_response.md)
- [parse_login_response](../../../../functions/crates/lpe-imap/src/auth/parse_login_response.md)
- [parse_xoauth2_initial_response](../../../../functions/crates/lpe-imap/src/auth/parse_xoauth2_initial_response.md)

# Imports

- `anyhow::{anyhow, bail, Result}`
- `base64::{engine::general_purpose::STANDARD as BASE64, Engine as _}`
- `lpe_magika::Detector`
- `lpe_mail_auth::{authenticate_bearer_access_token, authenticate_plain_credentials}`
- `tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader}`
- `crate::{parse::tokenize, Session}`

# Member of

- [lpe-imap](../../../../packages/crates/lpe-imap.md)