---
type: Rust Function
title: commit_existing_contact_import_in_tx
resource: crates/lpe-storage/src/mapi_contacts.rs#L564-L723
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/mapi_contacts/lock_contact_replica_in_tx
  - functions/crates/lpe-storage/src/mapi_contacts/imported_source_counter
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/mapi_contacts/parse_contact_predecessor_change_list
  - functions/crates/lpe-storage/src/mapi_contacts/contact_predecessors_include
  - functions/crates/lpe-storage/src/mapi_contacts/contact_predecessors_contain_change_key
  - functions/crates/lpe-storage/src/mapi_contacts/allocate_next_contact_change_number_in_tx
  - functions/crates/lpe-storage/src/mapi_contacts/merge_contact_predecessor_change_lists
  - functions/crates/lpe-storage/src/mapi_contacts/normalize_filetime
  - functions/crates/lpe-storage/src/mapi_contacts/imported_contact_version_wins_last_writer
  called_by:
  - functions/crates/lpe-storage/src/mapi_contacts/Storage/create_mapi_contact
---

# Signature

`async fn commit_existing_contact_import_in_tx( tx: &mut sqlx::Transaction<'_, Postgres>, tenant_id: &Uuid, principal_account_id: Uuid, owner_account_id: Uuid, contact_book_id: Uuid, imported: &MapiContactImportedIdentity, fail_on_conflict: bool, ) -> Result<Option<ExistingContactIdentityCommit>>`

# Calls

- [lock_contact_replica_in_tx](../../../../../functions/crates/lpe-storage/src/mapi_contacts/lock_contact_replica_in_tx.md)
- [imported_source_counter](../../../../../functions/crates/lpe-storage/src/mapi_contacts/imported_source_counter.md)
- [query](../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [parse_contact_predecessor_change_list](../../../../../functions/crates/lpe-storage/src/mapi_contacts/parse_contact_predecessor_change_list.md)
- [contact_predecessors_include](../../../../../functions/crates/lpe-storage/src/mapi_contacts/contact_predecessors_include.md)
- [contact_predecessors_contain_change_key](../../../../../functions/crates/lpe-storage/src/mapi_contacts/contact_predecessors_contain_change_key.md)
- [allocate_next_contact_change_number_in_tx](../../../../../functions/crates/lpe-storage/src/mapi_contacts/allocate_next_contact_change_number_in_tx.md)
- [merge_contact_predecessor_change_lists](../../../../../functions/crates/lpe-storage/src/mapi_contacts/merge_contact_predecessor_change_lists.md)
- [normalize_filetime](../../../../../functions/crates/lpe-storage/src/mapi_contacts/normalize_filetime.md)
- [imported_contact_version_wins_last_writer](../../../../../functions/crates/lpe-storage/src/mapi_contacts/imported_contact_version_wins_last_writer.md)

# Called by

- [create_mapi_contact](../../../../../functions/crates/lpe-storage/src/mapi_contacts/Storage/create_mapi_contact.md)