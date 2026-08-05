---
type: Rust Function
title: logical_quota_snapshot
resource: crates/lpe-storage/src/blob_store/tests.rs#L259-L280
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/Parser/expect
  - functions/crates/lpe-storage/src/jmap_blobs/Storage/fetch_mailbox_logical_quota_used_octets
  - functions/crates/lpe-storage/src/jmap_blobs/Storage/fetch_domain_logical_quota_used_octets
  called_by:
  - functions/crates/lpe-storage/src/blob_store/tests/logical_quota_is_stable_across_deduplicated_blob_migration
---

# Signature

`async fn logical_quota_snapshot( storage: &Storage, tenant_id: Uuid, domain_id: Uuid, account_id: Uuid, mailbox_id: Uuid, ) -> (u64, u64, u64)`

# Calls

- [expect](../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
- [fetch_mailbox_logical_quota_used_octets](../../../../../../functions/crates/lpe-storage/src/jmap_blobs/Storage/fetch_mailbox_logical_quota_used_octets.md)
- [fetch_domain_logical_quota_used_octets](../../../../../../functions/crates/lpe-storage/src/jmap_blobs/Storage/fetch_domain_logical_quota_used_octets.md)

# Called by

- [logical_quota_is_stable_across_deduplicated_blob_migration](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/logical_quota_is_stable_across_deduplicated_blob_migration.md)