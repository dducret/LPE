---
type: Rust Function
title: mapi_event_identity_object_kind
resource: crates/lpe-storage/src/mapi_events.rs#L814-L820
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/mapi_events/Storage/commit_mapi_event_update
  - functions/crates/lpe-storage/src/mapi_events/Storage/advance_mapi_event_version_for_lifecycle_in_tx
---

# Signature

`fn mapi_event_identity_object_kind(lifecycle_state: &str) -> Result<&'static str>`

# Called by

- [commit_mapi_event_update](../../../../../functions/crates/lpe-storage/src/mapi_events/Storage/commit_mapi_event_update.md)
- [advance_mapi_event_version_for_lifecycle_in_tx](../../../../../functions/crates/lpe-storage/src/mapi_events/Storage/advance_mapi_event_version_for_lifecycle_in_tx.md)