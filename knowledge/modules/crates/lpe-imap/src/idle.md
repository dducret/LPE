---
type: Rust Module
title: idle
resource: crates/lpe-imap/src/idle.rs#L1-L66
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-bail-result
  - external/lpe-magika-detector
  - external/tokio-io-asyncbufreadext-asyncreadext-asyncwriteext-bufreader-time-timeout-duration
  - external/crate-render-render-selected-updates-session
  member_of:
  - packages/crates/lpe-imap
---

# Contains

- [handle_idle](../../../../functions/crates/lpe-imap/src/idle/Session/handle_idle.md)

# Imports

- `anyhow::{bail, Result}`
- `lpe_magika::Detector`
- `tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    time::{timeout, Duration},
}`
- `crate::{render::render_selected_updates, Session}`

# Member of

- [lpe-imap](../../../../packages/crates/lpe-imap.md)