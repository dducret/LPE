---
type: Rust Function
title: insert_logical_message_with_attachment
resource: crates/lpe-storage/src/blob_store/tests.rs#L155-L257
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/core/Storage/pool
  - functions/crates/lpe-core/src/sieve/Parser/expect
  - functions/crates/lpe-storage/src/shared/Storage/store_message_blob_in_tx
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-domain/src/crypto/sha256_hex
  - functions/crates/lpe-storage/src/attachments/Storage/ingest_message_attachments_in_tx
  - functions/crates/lpe-storage/src/shared/Storage/assign_message_attachments_membership_in_tx
  called_by:
  - functions/crates/lpe-storage/src/blob_store/tests/logical_quota_is_stable_across_deduplicated_blob_migration
  - functions/crates/lpe-storage/src/blob_store/tests/retiring_placement_cleanup_is_blocked_when_live_references_need_it
  - functions/crates/lpe-storage/src/blob_store/tests/retiring_placement_cleanup_is_blocked_by_retention_and_legal_hold
---

# Signature

`async fn insert_logical_message_with_attachment( storage: &Storage, tenant_id: Uuid, domain_id: Uuid, account_id: Uuid, mailbox_id: Uuid, imap_uid: i64, logical_size_octets: i64, attachment_bytes: &[u8], ) -> Uuid`

# Calls

- [pool](../../../../../../functions/crates/lpe-storage/src/core/Storage/pool.md)
- [expect](../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
- [store_message_blob_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/store_message_blob_in_tx.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [sha256_hex](../../../../../../functions/crates/lpe-domain/src/crypto/sha256_hex.md)
- [ingest_message_attachments_in_tx](../../../../../../functions/crates/lpe-storage/src/attachments/Storage/ingest_message_attachments_in_tx.md)
- [assign_message_attachments_membership_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/assign_message_attachments_membership_in_tx.md)

# Called by

- [logical_quota_is_stable_across_deduplicated_blob_migration](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/logical_quota_is_stable_across_deduplicated_blob_migration.md)
- [retiring_placement_cleanup_is_blocked_when_live_references_need_it](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/retiring_placement_cleanup_is_blocked_when_live_references_need_it.md)
- [retiring_placement_cleanup_is_blocked_by_retention_and_legal_hold](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/retiring_placement_cleanup_is_blocked_by_retention_and_legal_hold.md)