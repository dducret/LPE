---
type: Rust Method
title: refresh_selected_updates
resource: crates/lpe-imap/src/service.rs#L474-L491
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-imap/src/service/Session/refresh_selected
  - functions/crates/lpe-imap/src/render/render_selected_updates
  called_by:
  - functions/crates/lpe-imap/src/mailboxes/Session/handle_check
  - functions/crates/lpe-imap/src/service/Session/handle_noop
---

# Signature

`pub(crate) async fn refresh_selected_updates<W>(&mut self, writer: &mut W) -> Result<()> where W: AsyncWriteExt + Unpin,`

# Calls

- [refresh_selected](../../../../../../functions/crates/lpe-imap/src/service/Session/refresh_selected.md)
- [render_selected_updates](../../../../../../functions/crates/lpe-imap/src/render/render_selected_updates.md)

# Called by

- [handle_check](../../../../../../functions/crates/lpe-imap/src/mailboxes/Session/handle_check.md)
- [handle_noop](../../../../../../functions/crates/lpe-imap/src/service/Session/handle_noop.md)