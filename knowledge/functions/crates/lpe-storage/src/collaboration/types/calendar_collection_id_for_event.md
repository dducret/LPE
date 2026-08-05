---
type: Rust Function
title: calendar_collection_id_for_event
resource: crates/lpe-storage/src/collaboration/types.rs#L349-L365
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/collaboration/types/collection_id_for_owner
  called_by:
  - functions/crates/lpe-storage/src/collaboration/Storage/fetch_accessible_events_internal
---

# Signature

`pub(super) fn calendar_collection_id_for_event( principal_account_id: Uuid, owner_account_id: Uuid, calendar_id: Uuid, role: &str, ) -> String`

# Calls

- [collection_id_for_owner](../../../../../../functions/crates/lpe-storage/src/collaboration/types/collection_id_for_owner.md)

# Called by

- [fetch_accessible_events_internal](../../../../../../functions/crates/lpe-storage/src/collaboration/Storage/fetch_accessible_events_internal.md)