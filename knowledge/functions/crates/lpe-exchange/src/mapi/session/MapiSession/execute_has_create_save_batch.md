---
type: Rust Method
title: execute_has_create_save_batch
resource: crates/lpe-exchange/src/mapi/session.rs#L1142-L1152
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_visible_inbox_open_create_save_batch
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_post_visible_inbox_release_create_save_batch
---

# Signature

`fn execute_has_create_save_batch(rop_ids: &[u8]) -> bool`

# Called by

- [record_visible_inbox_open_create_save_batch](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_visible_inbox_open_create_save_batch.md)
- [record_post_visible_inbox_release_create_save_batch](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_post_visible_inbox_release_create_save_batch.md)