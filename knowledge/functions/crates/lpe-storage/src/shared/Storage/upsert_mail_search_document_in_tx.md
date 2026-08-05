---
type: Rust Method
title: upsert_mail_search_document_in_tx
resource: crates/lpe-storage/src/shared.rs#L630-L672
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  called_by:
  - functions/crates/lpe-storage/src/inbound/Storage/store_inbound_message_in_tx
  - functions/crates/lpe-storage/src/message_ops/Storage/import_jmap_email
  - functions/crates/lpe-storage/src/pst/Storage/persist_pst_imported_message_in_tx
  - functions/crates/lpe-storage/src/submission/Storage/save_draft_message
  - functions/crates/lpe-storage/src/submission/Storage/submit_message
---

# Signature

`pub(crate) async fn upsert_mail_search_document_in_tx( tx: &mut sqlx::Transaction<'_, Postgres>, tenant_id: &Uuid, account_id: Uuid, mailbox_message_id: Uuid, message_id: Uuid, subject_text: &str, participants_visible: &str, body_text: &str, attachment_text: &str, ) -> Result<()>`

# Calls

- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)

# Called by

- [store_inbound_message_in_tx](../../../../../../functions/crates/lpe-storage/src/inbound/Storage/store_inbound_message_in_tx.md)
- [import_jmap_email](../../../../../../functions/crates/lpe-storage/src/message_ops/Storage/import_jmap_email.md)
- [persist_pst_imported_message_in_tx](../../../../../../functions/crates/lpe-storage/src/pst/Storage/persist_pst_imported_message_in_tx.md)
- [save_draft_message](../../../../../../functions/crates/lpe-storage/src/submission/Storage/save_draft_message.md)
- [submit_message](../../../../../../functions/crates/lpe-storage/src/submission/Storage/submit_message.md)