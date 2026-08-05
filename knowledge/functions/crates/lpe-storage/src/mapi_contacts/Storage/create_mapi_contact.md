---
type: Rust Method
title: create_mapi_contact
resource: crates/lpe-storage/src/mapi_contacts.rs#L121-L366
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/mapi_contacts/NormalizedContact/from_input
  - functions/crates/lpe-storage/src/mapi_contacts/validate_custom_properties
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-storage/src/mapi_contacts/contact_book_role
  - functions/crates/lpe-storage/src/core/Storage/pool
  - functions/crates/lpe-storage/src/collaboration/Storage/ensure_contact_book_in_tx
  - functions/crates/lpe-storage/src/mapi_contacts/recheck_contact_write_access_in_tx
  - functions/crates/lpe-storage/src/mapi_contacts/commit_existing_contact_import_in_tx
  - functions/crates/lpe-storage/src/mapi_contacts/update_contact_in_tx
  - functions/crates/lpe-storage/src/mapi_contacts/upsert_custom_properties_in_tx
  - functions/crates/lpe-storage/src/mapi_contacts/record_contact_change_in_tx
  - functions/crates/lpe-core/src/sieve/Parser/next
  - functions/crates/lpe-storage/src/mapi_contacts/normalize_filetime
  - functions/crates/lpe-storage/src/mapi_contacts/current_filetime_in_tx
  - functions/crates/lpe-storage/src/mapi_contacts/allocate_contact_identity_in_tx
  - functions/crates/lpe-storage/src/mapi_contacts/insert_contact_in_tx
  - functions/crates/lpe-storage/src/mapi_contacts/insert_custom_properties_in_tx
  - functions/crates/lpe-storage/src/shared/Storage/allocate_account_modseq_in_tx
  - functions/crates/lpe-storage/src/mapi_contacts/set_created_contact_modseq_in_tx
  - functions/crates/lpe-storage/src/mapi_contacts/contact_affected_principals_in_tx
  - functions/crates/lpe-storage/src/shared/Storage/insert_mail_change_log_in_tx
  - functions/crates/lpe-storage/src/change/Storage/emit_collaboration_change
---

# Signature

`pub async fn create_mapi_contact( &self, input: MapiContactCreateInput, ) -> Result<MapiContactCreateResult>`

# Calls

- [from_input](../../../../../../functions/crates/lpe-storage/src/mapi_contacts/NormalizedContact/from_input.md)
- [validate_custom_properties](../../../../../../functions/crates/lpe-storage/src/mapi_contacts/validate_custom_properties.md)
- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [contact_book_role](../../../../../../functions/crates/lpe-storage/src/mapi_contacts/contact_book_role.md)
- [pool](../../../../../../functions/crates/lpe-storage/src/core/Storage/pool.md)
- [ensure_contact_book_in_tx](../../../../../../functions/crates/lpe-storage/src/collaboration/Storage/ensure_contact_book_in_tx.md)
- [recheck_contact_write_access_in_tx](../../../../../../functions/crates/lpe-storage/src/mapi_contacts/recheck_contact_write_access_in_tx.md)
- [commit_existing_contact_import_in_tx](../../../../../../functions/crates/lpe-storage/src/mapi_contacts/commit_existing_contact_import_in_tx.md)
- [update_contact_in_tx](../../../../../../functions/crates/lpe-storage/src/mapi_contacts/update_contact_in_tx.md)
- [upsert_custom_properties_in_tx](../../../../../../functions/crates/lpe-storage/src/mapi_contacts/upsert_custom_properties_in_tx.md)
- [record_contact_change_in_tx](../../../../../../functions/crates/lpe-storage/src/mapi_contacts/record_contact_change_in_tx.md)
- [next](../../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)
- [normalize_filetime](../../../../../../functions/crates/lpe-storage/src/mapi_contacts/normalize_filetime.md)
- [current_filetime_in_tx](../../../../../../functions/crates/lpe-storage/src/mapi_contacts/current_filetime_in_tx.md)
- [allocate_contact_identity_in_tx](../../../../../../functions/crates/lpe-storage/src/mapi_contacts/allocate_contact_identity_in_tx.md)
- [insert_contact_in_tx](../../../../../../functions/crates/lpe-storage/src/mapi_contacts/insert_contact_in_tx.md)
- [insert_custom_properties_in_tx](../../../../../../functions/crates/lpe-storage/src/mapi_contacts/insert_custom_properties_in_tx.md)
- [allocate_account_modseq_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/allocate_account_modseq_in_tx.md)
- [set_created_contact_modseq_in_tx](../../../../../../functions/crates/lpe-storage/src/mapi_contacts/set_created_contact_modseq_in_tx.md)
- [contact_affected_principals_in_tx](../../../../../../functions/crates/lpe-storage/src/mapi_contacts/contact_affected_principals_in_tx.md)
- [insert_mail_change_log_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/insert_mail_change_log_in_tx.md)
- [emit_collaboration_change](../../../../../../functions/crates/lpe-storage/src/change/Storage/emit_collaboration_change.md)