---
type: Rust Method
title: upsert_client_contact_in_book_role
resource: crates/lpe-storage/src/workspace.rs#L381-L628
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/Parser/next
  - functions/crates/lpe-storage/src/workspace/merge_contact_update_input
  - functions/crates/lpe-storage/src/workspace/contact_emails_json
  - functions/crates/lpe-storage/src/workspace/contact_primary_email
  - functions/crates/lpe-storage/src/workspace/contact_phones_json
  - functions/crates/lpe-storage/src/workspace/contact_array_json
  - functions/crates/lpe-storage/src/workspace/contact_source_payload_json
  - functions/crates/lpe-storage/src/workspace/contact_update_is_unchanged
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-storage/src/collaboration/Storage/ensure_contact_book_in_tx
  - functions/crates/lpe-storage/src/workspace/client_address_book_id_for_role
  - functions/crates/lpe-storage/src/shared/Storage/allocate_account_modseq_in_tx
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/mapi_contacts/Storage/rotate_active_mapi_contact_identities_in_tx
  - functions/crates/lpe-storage/src/shared/Storage/insert_mail_change_log_in_tx
  - functions/crates/lpe-storage/src/change/Storage/emit_collaboration_change
  - functions/crates/lpe-storage/src/workspace/map_contact
  called_by:
  - functions/crates/lpe-storage/src/collaboration/Storage/create_accessible_contact
  - functions/crates/lpe-storage/src/collaboration/Storage/update_accessible_contact
  - functions/crates/lpe-storage/src/workspace/Storage/upsert_client_contact
---

# Signature

`pub(crate) async fn upsert_client_contact_in_book_role( &self, mut input: UpsertClientContactInput, contact_book_role: &str, ) -> Result<ClientContact>`

# Calls

- [next](../../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)
- [merge_contact_update_input](../../../../../../functions/crates/lpe-storage/src/workspace/merge_contact_update_input.md)
- [contact_emails_json](../../../../../../functions/crates/lpe-storage/src/workspace/contact_emails_json.md)
- [contact_primary_email](../../../../../../functions/crates/lpe-storage/src/workspace/contact_primary_email.md)
- [contact_phones_json](../../../../../../functions/crates/lpe-storage/src/workspace/contact_phones_json.md)
- [contact_array_json](../../../../../../functions/crates/lpe-storage/src/workspace/contact_array_json.md)
- [contact_source_payload_json](../../../../../../functions/crates/lpe-storage/src/workspace/contact_source_payload_json.md)
- [contact_update_is_unchanged](../../../../../../functions/crates/lpe-storage/src/workspace/contact_update_is_unchanged.md)
- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [ensure_contact_book_in_tx](../../../../../../functions/crates/lpe-storage/src/collaboration/Storage/ensure_contact_book_in_tx.md)
- [client_address_book_id_for_role](../../../../../../functions/crates/lpe-storage/src/workspace/client_address_book_id_for_role.md)
- [allocate_account_modseq_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/allocate_account_modseq_in_tx.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [rotate_active_mapi_contact_identities_in_tx](../../../../../../functions/crates/lpe-storage/src/mapi_contacts/Storage/rotate_active_mapi_contact_identities_in_tx.md)
- [insert_mail_change_log_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/insert_mail_change_log_in_tx.md)
- [emit_collaboration_change](../../../../../../functions/crates/lpe-storage/src/change/Storage/emit_collaboration_change.md)
- [map_contact](../../../../../../functions/crates/lpe-storage/src/workspace/map_contact.md)

# Called by

- [create_accessible_contact](../../../../../../functions/crates/lpe-storage/src/collaboration/Storage/create_accessible_contact.md)
- [update_accessible_contact](../../../../../../functions/crates/lpe-storage/src/collaboration/Storage/update_accessible_contact.md)
- [upsert_client_contact](../../../../../../functions/crates/lpe-storage/src/workspace/Storage/upsert_client_contact.md)