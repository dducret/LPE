---
type: Rust Function
title: cleanup_blockers
resource: crates/lpe-storage/src/blob_store/tests.rs#L323-L329
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/old_placement_cleanup_eligibility
  - functions/crates/lpe-storage/src/core/Storage/pool
  - functions/crates/lpe-core/src/sieve/Parser/expect
  called_by:
  - functions/crates/lpe-storage/src/blob_store/tests/retiring_placement_cleanup_is_blocked_when_live_references_need_it
  - functions/crates/lpe-storage/src/blob_store/tests/retiring_placement_cleanup_is_blocked_by_retention_and_legal_hold
---

# Signature

`async fn cleanup_blockers(storage: &Storage, placement_id: Uuid) -> Vec<String>`

# Calls

- [old_placement_cleanup_eligibility](../../../../../../functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/old_placement_cleanup_eligibility.md)
- [pool](../../../../../../functions/crates/lpe-storage/src/core/Storage/pool.md)
- [expect](../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)

# Called by

- [retiring_placement_cleanup_is_blocked_when_live_references_need_it](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/retiring_placement_cleanup_is_blocked_when_live_references_need_it.md)
- [retiring_placement_cleanup_is_blocked_by_retention_and_legal_hold](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/retiring_placement_cleanup_is_blocked_by_retention_and_legal_hold.md)