---
type: Rust Function
title: render_modified_set
resource: crates/lpe-imap/src/render.rs#L1237-L1253
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-imap/src/messages/Session/handle_store
---

# Signature

`pub(crate) fn render_modified_set( selected: &SelectedMailbox, modified_ids: &[Uuid], ref_kind: MessageRefKind, ) -> String`

# Calls

- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [handle_store](../../../../../functions/crates/lpe-imap/src/messages/Session/handle_store.md)