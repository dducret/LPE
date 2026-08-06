---
type: Rust Method
title: commit_mapi_contact_update
resource: crates/lpe-storage/src/mapi_contacts.rs#L472-L652
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/mapi_contacts/validate_mapi_contact_commit_input
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-storage/src/core/Storage/pool
  - functions/crates/lpe-activesync/src/tests/query
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-storage/src/mapi_contacts/NormalizedContact/from_input
  - functions/crates/lpe-storage/src/mapi_contacts/current_filetime_in_tx
  - functions/crates/lpe-storage/src/mapi_contacts/update_contact_in_tx
  - functions/crates/lpe-storage/src/mapi_contacts/upsert_custom_properties_in_tx
  - functions/crates/lpe-storage/src/mapi_contacts/delete_custom_properties_in_tx
  - functions/crates/lpe-storage/src/shared/Storage/allocate_account_modseq_in_tx
  - functions/crates/lpe-storage/src/mapi_contacts/set_created_contact_modseq_in_tx
  - functions/crates/lpe-storage/src/mapi_contacts/Storage/rotate_active_mapi_contact_identities_in_tx
  - functions/crates/lpe-storage/src/mapi_contacts/fetch_principal_contact_identity_in_tx
  - functions/crates/lpe-storage/src/mapi_contacts/contact_affected_principals_in_tx
  - functions/crates/lpe-storage/src/shared/Storage/insert_mail_change_log_in_tx
  - functions/crates/lpe-storage/src/change/Storage/emit_collaboration_change
  - functions/crates/lpe-core/src/sieve/Parser/next
---

# Signature

`pub async fn commit_mapi_contact_update( &self, input: MapiContactCommitInput, ) -> Result<MapiContactCommitOutcome>`

# Calls

- [validate_mapi_contact_commit_input](../../../../../../functions/crates/lpe-storage/src/mapi_contacts/validate_mapi_contact_commit_input.md)
- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [pool](../../../../../../functions/crates/lpe-storage/src/core/Storage/pool.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [get](../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [from_input](../../../../../../functions/crates/lpe-storage/src/mapi_contacts/NormalizedContact/from_input.md)
- [current_filetime_in_tx](../../../../../../functions/crates/lpe-storage/src/mapi_contacts/current_filetime_in_tx.md)
- [update_contact_in_tx](../../../../../../functions/crates/lpe-storage/src/mapi_contacts/update_contact_in_tx.md)
- [upsert_custom_properties_in_tx](../../../../../../functions/crates/lpe-storage/src/mapi_contacts/upsert_custom_properties_in_tx.md)
- [delete_custom_properties_in_tx](../../../../../../functions/crates/lpe-storage/src/mapi_contacts/delete_custom_properties_in_tx.md)
- [allocate_account_modseq_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/allocate_account_modseq_in_tx.md)
- [set_created_contact_modseq_in_tx](../../../../../../functions/crates/lpe-storage/src/mapi_contacts/set_created_contact_modseq_in_tx.md)
- [rotate_active_mapi_contact_identities_in_tx](../../../../../../functions/crates/lpe-storage/src/mapi_contacts/Storage/rotate_active_mapi_contact_identities_in_tx.md)
- [fetch_principal_contact_identity_in_tx](../../../../../../functions/crates/lpe-storage/src/mapi_contacts/fetch_principal_contact_identity_in_tx.md)
- [contact_affected_principals_in_tx](../../../../../../functions/crates/lpe-storage/src/mapi_contacts/contact_affected_principals_in_tx.md)
- [insert_mail_change_log_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/insert_mail_change_log_in_tx.md)
- [emit_collaboration_change](../../../../../../functions/crates/lpe-storage/src/change/Storage/emit_collaboration_change.md)
- [next](../../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)