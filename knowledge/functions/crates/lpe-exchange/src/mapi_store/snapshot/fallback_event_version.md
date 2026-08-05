---
type: Rust Function
title: fallback_event_version
resource: crates/lpe-exchange/src/mapi_store/snapshot.rs#L1558-L1568
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/change_number_for_store_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/remember_created_event
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/build
---

# Signature

`fn fallback_event_version(event: &AccessibleEvent, event_id: u64) -> MapiEventVersion`

# Calls

- [change_number_for_store_id](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/change_number_for_store_id.md)

# Called by

- [remember_created_event](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/remember_created_event.md)
- [build](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/build.md)