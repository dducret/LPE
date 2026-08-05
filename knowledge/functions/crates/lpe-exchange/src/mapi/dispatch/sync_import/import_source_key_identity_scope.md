---
type: Rust Function
title: import_source_key_identity_scope
resource: crates/lpe-exchange/src/mapi/dispatch/sync_import.rs#L783-L791
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/persistable_import_source_key_global_counter
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/pending_message_is_trash_sync_artifact
---

# Signature

`pub(super) fn import_source_key_identity_scope(counter: u64) -> &'static str`

# Called by

- [persistable_import_source_key_global_counter](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/persistable_import_source_key_global_counter.md)
- [pending_message_is_trash_sync_artifact](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/pending_message_is_trash_sync_artifact.md)