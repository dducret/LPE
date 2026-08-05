---
type: Rust Function
title: sha256_hex
resource: crates/lpe-domain/src/crypto.rs#L17-L19
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-domain/src/crypto/hex_lower
  called_by:
  - functions/crates/lpe-domain/src/bridge_auth/sign_components
  - functions/crates/lpe-domain/src/crypto/sha256_hex_prefix
  - functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/put_durable_blob_in_tx
  - functions/crates/lpe-storage/src/blob_store/tests/insert_logical_message_with_attachment
  - functions/crates/lpe-storage/src/blob_store/tests/retiring_placement_cleanup_is_blocked_when_live_references_need_it
  - functions/crates/lpe-storage/src/blob_store/tests/attachment_content_fetch_reads_through_blob_store_boundary
  - functions/crates/lpe-storage/src/inbound/Storage/store_inbound_message_in_tx
  - functions/crates/lpe-storage/src/jmap_blobs/Storage/save_jmap_upload_blob
  - functions/crates/lpe-storage/src/message_ops/Storage/import_jmap_email
  - functions/crates/lpe-storage/src/pst/Storage/persist_pst_imported_message_in_tx
  - functions/crates/lpe-storage/src/pst/insert_message_with_attachment
  - functions/crates/lpe-storage/src/shared/Storage/store_message_blob_in_tx
  - functions/crates/lpe-storage/src/shared/Storage/upsert_message_body_in_tx
  - functions/crates/lpe-storage/src/storage_backend/s3_put_object
  - functions/crates/lpe-storage/src/storage_visibility/tests/s3_compatible_pool_health_checks_active_object_placement
  - functions/crates/lpe-storage/src/submission/Storage/save_draft_message
  - functions/crates/lpe-storage/src/submission/Storage/submit_message
---

# Signature

`pub fn sha256_hex(bytes: &[u8]) -> String`

# Calls

- [hex_lower](../../../../../functions/crates/lpe-domain/src/crypto/hex_lower.md)

# Called by

- [sign_components](../../../../../functions/crates/lpe-domain/src/bridge_auth/sign_components.md)
- [sha256_hex_prefix](../../../../../functions/crates/lpe-domain/src/crypto/sha256_hex_prefix.md)
- [put_durable_blob_in_tx](../../../../../functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/put_durable_blob_in_tx.md)
- [insert_logical_message_with_attachment](../../../../../functions/crates/lpe-storage/src/blob_store/tests/insert_logical_message_with_attachment.md)
- [retiring_placement_cleanup_is_blocked_when_live_references_need_it](../../../../../functions/crates/lpe-storage/src/blob_store/tests/retiring_placement_cleanup_is_blocked_when_live_references_need_it.md)
- [attachment_content_fetch_reads_through_blob_store_boundary](../../../../../functions/crates/lpe-storage/src/blob_store/tests/attachment_content_fetch_reads_through_blob_store_boundary.md)
- [store_inbound_message_in_tx](../../../../../functions/crates/lpe-storage/src/inbound/Storage/store_inbound_message_in_tx.md)
- [save_jmap_upload_blob](../../../../../functions/crates/lpe-storage/src/jmap_blobs/Storage/save_jmap_upload_blob.md)
- [import_jmap_email](../../../../../functions/crates/lpe-storage/src/message_ops/Storage/import_jmap_email.md)
- [persist_pst_imported_message_in_tx](../../../../../functions/crates/lpe-storage/src/pst/Storage/persist_pst_imported_message_in_tx.md)
- [insert_message_with_attachment](../../../../../functions/crates/lpe-storage/src/pst/insert_message_with_attachment.md)
- [store_message_blob_in_tx](../../../../../functions/crates/lpe-storage/src/shared/Storage/store_message_blob_in_tx.md)
- [upsert_message_body_in_tx](../../../../../functions/crates/lpe-storage/src/shared/Storage/upsert_message_body_in_tx.md)
- [s3_put_object](../../../../../functions/crates/lpe-storage/src/storage_backend/s3_put_object.md)
- [s3_compatible_pool_health_checks_active_object_placement](../../../../../functions/crates/lpe-storage/src/storage_visibility/tests/s3_compatible_pool_health_checks_active_object_placement.md)
- [save_draft_message](../../../../../functions/crates/lpe-storage/src/submission/Storage/save_draft_message.md)
- [submit_message](../../../../../functions/crates/lpe-storage/src/submission/Storage/submit_message.md)