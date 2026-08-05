---
type: Rust Method
title: old_placement_cleanup_eligibility
resource: crates/lpe-storage/src/blob_store.rs#L890-L997
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/live_reference_cleanup_blockers
  - functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/message_lifecycle_cleanup_blockers
  called_by:
  - functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/cleanup_one_old_placement_inner
  - functions/crates/lpe-storage/src/blob_store/tests/cleanup_blockers
  - functions/crates/lpe-storage/src/blob_store/tests/retiring_placement_cleanup_is_blocked_by_rollback_window
---

# Signature

`pub(crate) async fn old_placement_cleanup_eligibility( &self, pool: &PgPool, placement_id: Uuid, ) -> Result<PlacementCleanupEligibility>`

# Calls

- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [live_reference_cleanup_blockers](../../../../../../functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/live_reference_cleanup_blockers.md)
- [message_lifecycle_cleanup_blockers](../../../../../../functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/message_lifecycle_cleanup_blockers.md)

# Called by

- [cleanup_one_old_placement_inner](../../../../../../functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/cleanup_one_old_placement_inner.md)
- [cleanup_blockers](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/cleanup_blockers.md)
- [retiring_placement_cleanup_is_blocked_by_rollback_window](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/retiring_placement_cleanup_is_blocked_by_rollback_window.md)