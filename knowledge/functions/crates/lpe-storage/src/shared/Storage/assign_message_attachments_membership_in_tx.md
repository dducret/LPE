---
type: Rust Method
title: assign_message_attachments_membership_in_tx
resource: crates/lpe-storage/src/shared.rs#L603-L628
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  called_by:
  - functions/crates/lpe-storage/src/attachments/Storage/add_message_attachment
  - functions/crates/lpe-storage/src/blob_store/tests/insert_logical_message_with_attachment
  - functions/crates/lpe-storage/src/inbound/Storage/store_inbound_message_in_tx
  - functions/crates/lpe-storage/src/message_ops/Storage/import_jmap_email
  - functions/crates/lpe-storage/src/pst/Storage/persist_pst_imported_message_in_tx
  - functions/crates/lpe-storage/src/pst/insert_message_with_attachment
  - functions/crates/lpe-storage/src/submission/Storage/save_draft_message
  - functions/crates/lpe-storage/src/submission/Storage/submit_message
---

# Signature

`pub(crate) async fn assign_message_attachments_membership_in_tx( &self, tx: &mut sqlx::Transaction<'_, Postgres>, tenant_id: &Uuid, account_id: Uuid, message_id: Uuid, mailbox_message_id: Uuid, ) -> Result<()>`

# Calls

- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)

# Called by

- [add_message_attachment](../../../../../../functions/crates/lpe-storage/src/attachments/Storage/add_message_attachment.md)
- [insert_logical_message_with_attachment](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/insert_logical_message_with_attachment.md)
- [store_inbound_message_in_tx](../../../../../../functions/crates/lpe-storage/src/inbound/Storage/store_inbound_message_in_tx.md)
- [import_jmap_email](../../../../../../functions/crates/lpe-storage/src/message_ops/Storage/import_jmap_email.md)
- [persist_pst_imported_message_in_tx](../../../../../../functions/crates/lpe-storage/src/pst/Storage/persist_pst_imported_message_in_tx.md)
- [insert_message_with_attachment](../../../../../../functions/crates/lpe-storage/src/pst/insert_message_with_attachment.md)
- [save_draft_message](../../../../../../functions/crates/lpe-storage/src/submission/Storage/save_draft_message.md)
- [submit_message](../../../../../../functions/crates/lpe-storage/src/submission/Storage/submit_message.md)