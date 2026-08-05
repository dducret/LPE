---
type: Rust Function
title: render_selected_updates
resource: crates/lpe-imap/src/render.rs#L1263-L1327
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-imap/src/idle/Session/handle_idle
  - functions/crates/lpe-imap/src/service/Session/refresh_selected_updates
---

# Signature

`pub(crate) fn render_selected_updates( previous: &SelectedMailbox, current: &SelectedMailbox, ) -> Result<String>`

# Called by

- [handle_idle](../../../../../functions/crates/lpe-imap/src/idle/Session/handle_idle.md)
- [refresh_selected_updates](../../../../../functions/crates/lpe-imap/src/service/Session/refresh_selected_updates.md)