---
type: Rust Method
title: handle_idle
resource: crates/lpe-imap/src/idle.rs#L11-L65
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-imap/src/service/Session/refresh_selected
  - functions/crates/lpe-imap/src/render/render_selected_updates
  called_by:
  - functions/crates/lpe-imap/src/service/Session/handle_request
---

# Signature

`pub(crate) async fn handle_idle<R, W>( &mut self, reader: &mut BufReader<R>, writer: &mut W, tag: &str, ) -> Result<bool> where R: AsyncReadExt + Unpin, W: AsyncWriteExt + Unpin,`

# Calls

- [refresh_selected](../../../../../../functions/crates/lpe-imap/src/service/Session/refresh_selected.md)
- [render_selected_updates](../../../../../../functions/crates/lpe-imap/src/render/render_selected_updates.md)

# Called by

- [handle_request](../../../../../../functions/crates/lpe-imap/src/service/Session/handle_request.md)