---
type: Rust Function
title: insert_message_with_attachment
resource: crates/lpe-storage/src/pst.rs#L684-L801
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
  - functions/crates/lpe-storage/src/shared/Storage/upsert_message_body_in_tx
  - functions/crates/lpe-storage/src/attachments/Storage/ingest_message_attachments_in_tx
  - functions/crates/lpe-storage/src/shared/Storage/assign_message_attachments_membership_in_tx
  called_by:
  - functions/crates/lpe-storage/src/pst/pst_export_reconstructs_attachment_after_old_placement_cleanup
---

# Signature

`async fn insert_message_with_attachment( storage: &Storage, tenant_id: Uuid, domain_id: Uuid, account_id: Uuid, mailbox_id: Uuid, ) -> (Uuid, Uuid)`

# Calls

- [pool](../../../../../functions/crates/lpe-storage/src/core/Storage/pool.md)
- [expect](../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
- [store_message_blob_in_tx](../../../../../functions/crates/lpe-storage/src/shared/Storage/store_message_blob_in_tx.md)
- [query](../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [sha256_hex](../../../../../functions/crates/lpe-domain/src/crypto/sha256_hex.md)
- [upsert_message_body_in_tx](../../../../../functions/crates/lpe-storage/src/shared/Storage/upsert_message_body_in_tx.md)
- [ingest_message_attachments_in_tx](../../../../../functions/crates/lpe-storage/src/attachments/Storage/ingest_message_attachments_in_tx.md)
- [assign_message_attachments_membership_in_tx](../../../../../functions/crates/lpe-storage/src/shared/Storage/assign_message_attachments_membership_in_tx.md)

# Called by

- [pst_export_reconstructs_attachment_after_old_placement_cleanup](../../../../../functions/crates/lpe-storage/src/pst/pst_export_reconstructs_attachment_after_old_placement_cleanup.md)