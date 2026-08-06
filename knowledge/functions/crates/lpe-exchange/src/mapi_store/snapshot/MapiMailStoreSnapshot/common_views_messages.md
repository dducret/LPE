---
type: Rust Method
title: common_views_messages
resource: crates/lpe-exchange/src/mapi_store/snapshot.rs#L1316-L1318
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/canonical_common_views_fai_messages
  called_by:
  - functions/crates/lpe-exchange/src/mapi/sync/common_views_sync_messages
  - functions/crates/lpe-exchange/src/mapi_store/tests/common_views_uses_same_persisted_wlinks_and_durable_ids_for_table_and_ics
---

# Signature

`pub(crate) fn common_views_messages(&self) -> impl Iterator<Item = MapiCommonViewsMessage>`

# Calls

- [canonical_common_views_fai_messages](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/canonical_common_views_fai_messages.md)

# Called by

- [common_views_sync_messages](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/common_views_sync_messages.md)
- [common_views_uses_same_persisted_wlinks_and_durable_ids_for_table_and_ics](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/common_views_uses_same_persisted_wlinks_and_durable_ids_for_table_and_ics.md)