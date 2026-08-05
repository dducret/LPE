---
type: Rust Function
title: common_views_sync_messages
resource: crates/lpe-exchange/src/mapi/sync.rs#L396-L400
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/common_views_messages
  called_by:
  - functions/crates/lpe-exchange/src/mapi/sync/special_sync_objects_for
---

# Signature

`fn common_views_sync_messages( snapshot: &MapiMailStoreSnapshot, ) -> Vec<crate::mapi_store::MapiCommonViewsMessage>`

# Calls

- [common_views_messages](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/common_views_messages.md)

# Called by

- [special_sync_objects_for](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/special_sync_objects_for.md)