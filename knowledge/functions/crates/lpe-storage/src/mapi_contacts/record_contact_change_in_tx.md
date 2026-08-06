---
type: Rust Function
title: record_contact_change_in_tx
resource: crates/lpe-storage/src/mapi_contacts.rs#L1358-L1416
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/allocate_account_modseq_in_tx
  - functions/crates/lpe-storage/src/mapi_contacts/set_created_contact_modseq_in_tx
  - functions/crates/lpe-storage/src/mapi_contacts/contact_affected_principals_in_tx
  - functions/crates/lpe-storage/src/shared/Storage/insert_mail_change_log_in_tx
  - functions/crates/lpe-storage/src/change/Storage/emit_collaboration_change
  called_by:
  - functions/crates/lpe-storage/src/mapi_contacts/Storage/create_mapi_contact
---

# Signature

`async fn record_contact_change_in_tx( storage: &Storage, tx: &mut sqlx::Transaction<'_, Postgres>, tenant_id: &Uuid, owner_account_id: Uuid, contact_book_id: Uuid, contact_id: Uuid, change_kind: &str, created: bool, custom_properties_changed: bool, mapi_change_number: u64, ) -> Result<()>`

# Calls

- [allocate_account_modseq_in_tx](../../../../../functions/crates/lpe-storage/src/shared/Storage/allocate_account_modseq_in_tx.md)
- [set_created_contact_modseq_in_tx](../../../../../functions/crates/lpe-storage/src/mapi_contacts/set_created_contact_modseq_in_tx.md)
- [contact_affected_principals_in_tx](../../../../../functions/crates/lpe-storage/src/mapi_contacts/contact_affected_principals_in_tx.md)
- [insert_mail_change_log_in_tx](../../../../../functions/crates/lpe-storage/src/shared/Storage/insert_mail_change_log_in_tx.md)
- [emit_collaboration_change](../../../../../functions/crates/lpe-storage/src/change/Storage/emit_collaboration_change.md)

# Called by

- [create_mapi_contact](../../../../../functions/crates/lpe-storage/src/mapi_contacts/Storage/create_mapi_contact.md)