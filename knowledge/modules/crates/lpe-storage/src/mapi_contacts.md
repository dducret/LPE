---
type: Rust Module
title: mapi_contacts
resource: crates/lpe-storage/src/mapi_contacts.rs#L1-L1423
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/std-collections-btreemap-hashset
  - external/anyhow-anyhow-bail-result
  - external/serde-json-value
  - external/sqlx-postgres-row
  - external/uuid-uuid
  - external/crate-mapi-events-mapi-change-key-mapi-store-id-merge-predecessor-change-list-first-dynamic-mapi-global-counter-first-reserved-high-global-counter-max-mapi-global-counter
  - external/crate-mapi-store-identity-allocate-mapi-store-global-counter-in-tx-ensure-mapi-mailbox-replica-in-tx-ensure-mapi-store-identity-in-tx
  - external/crate-workspace-contact-array-json-contact-emails-json-contact-phones-json-contact-primary-email-contact-source-payload-json
  - external/crate-accessiblecontact-canonicalchangecategory-contactnamefields-contactsourcefields-storage-upsertclientcontactinput-default-contact-book-role-im-contact-list-collection-id-im-contact-list-role-quick-contacts-collection-id-quick-contacts-role-suggested-contacts-collection-id-suggested-contacts-role
  - external/super
  member_of:
  - packages/crates/lpe-storage
---

# Contains

- [MapiContactCreateInput](../../../../classes/crates/lpe-storage/src/mapi_contacts/MapiContactCreateInput.md)
- [MapiContactImportedIdentity](../../../../classes/crates/lpe-storage/src/mapi_contacts/MapiContactImportedIdentity.md)
- [MapiContactCustomPropertyValue](../../../../classes/crates/lpe-storage/src/mapi_contacts/MapiContactCustomPropertyValue.md)
- [MapiContactVersion](../../../../classes/crates/lpe-storage/src/mapi_contacts/MapiContactVersion.md)
- [MapiContactCreateResult](../../../../classes/crates/lpe-storage/src/mapi_contacts/MapiContactCreateResult.md)
- [MapiContactImportDisposition](../../../../classes/crates/lpe-storage/src/mapi_contacts/MapiContactImportDisposition.md)
- [changes_server_replica](../../../../functions/crates/lpe-storage/src/mapi_contacts/MapiContactImportDisposition/changes_server_replica.md)
- [MapiContactImportConflict](../../../../classes/crates/lpe-storage/src/mapi_contacts/MapiContactImportConflict.md)
- [fmt](../../../../functions/crates/lpe-storage/src/mapi_contacts/MapiContactImportConflict/std-fmt-display/fmt.md)
- [MapiContactImportObjectDeleted](../../../../classes/crates/lpe-storage/src/mapi_contacts/MapiContactImportObjectDeleted.md)
- [fmt](../../../../functions/crates/lpe-storage/src/mapi_contacts/MapiContactImportObjectDeleted/std-fmt-display/fmt.md)
- [AllocatedContactIdentity](../../../../classes/crates/lpe-storage/src/mapi_contacts/AllocatedContactIdentity.md)
- [ExistingContactIdentityCommit](../../../../classes/crates/lpe-storage/src/mapi_contacts/ExistingContactIdentityCommit.md)
- [create_mapi_contact](../../../../functions/crates/lpe-storage/src/mapi_contacts/Storage/create_mapi_contact.md)
- [NormalizedContact](../../../../classes/crates/lpe-storage/src/mapi_contacts/NormalizedContact.md)
- [from_input](../../../../functions/crates/lpe-storage/src/mapi_contacts/NormalizedContact/from_input.md)
- [contact_book_role](../../../../functions/crates/lpe-storage/src/mapi_contacts/contact_book_role.md)
- [validate_custom_properties](../../../../functions/crates/lpe-storage/src/mapi_contacts/validate_custom_properties.md)
- [validate_imported_identity](../../../../functions/crates/lpe-storage/src/mapi_contacts/validate_imported_identity.md)
- [normalize_filetime](../../../../functions/crates/lpe-storage/src/mapi_contacts/normalize_filetime.md)
- [current_filetime_in_tx](../../../../functions/crates/lpe-storage/src/mapi_contacts/current_filetime_in_tx.md)
- [recheck_contact_write_access_in_tx](../../../../functions/crates/lpe-storage/src/mapi_contacts/recheck_contact_write_access_in_tx.md)
- [commit_existing_contact_import_in_tx](../../../../functions/crates/lpe-storage/src/mapi_contacts/commit_existing_contact_import_in_tx.md)
- [imported_contact_version_wins_last_writer](../../../../functions/crates/lpe-storage/src/mapi_contacts/imported_contact_version_wins_last_writer.md)
- [parse_contact_predecessor_change_list](../../../../functions/crates/lpe-storage/src/mapi_contacts/parse_contact_predecessor_change_list.md)
- [split_contact_xid](../../../../functions/crates/lpe-storage/src/mapi_contacts/split_contact_xid.md)
- [contact_predecessors_include](../../../../functions/crates/lpe-storage/src/mapi_contacts/contact_predecessors_include.md)
- [contact_predecessors_contain_change_key](../../../../functions/crates/lpe-storage/src/mapi_contacts/contact_predecessors_contain_change_key.md)
- [merge_contact_predecessor_change_lists](../../../../functions/crates/lpe-storage/src/mapi_contacts/merge_contact_predecessor_change_lists.md)
- [lock_contact_replica_in_tx](../../../../functions/crates/lpe-storage/src/mapi_contacts/lock_contact_replica_in_tx.md)
- [allocate_next_contact_change_number_in_tx](../../../../functions/crates/lpe-storage/src/mapi_contacts/allocate_next_contact_change_number_in_tx.md)
- [allocate_contact_identity_in_tx](../../../../functions/crates/lpe-storage/src/mapi_contacts/allocate_contact_identity_in_tx.md)
- [imported_source_counter](../../../../functions/crates/lpe-storage/src/mapi_contacts/imported_source_counter.md)
- [imported_identity](../../../../functions/crates/lpe-storage/src/mapi_contacts/imported_identity.md)
- [imported_contact_change_key_must_use_a_foreign_identifier](../../../../functions/crates/lpe-storage/src/mapi_contacts/imported_contact_change_key_must_use_a_foreign_identifier.md)
- [insert_contact_in_tx](../../../../functions/crates/lpe-storage/src/mapi_contacts/insert_contact_in_tx.md)
- [update_contact_in_tx](../../../../functions/crates/lpe-storage/src/mapi_contacts/update_contact_in_tx.md)
- [insert_custom_properties_in_tx](../../../../functions/crates/lpe-storage/src/mapi_contacts/insert_custom_properties_in_tx.md)
- [upsert_custom_properties_in_tx](../../../../functions/crates/lpe-storage/src/mapi_contacts/upsert_custom_properties_in_tx.md)
- [record_contact_change_in_tx](../../../../functions/crates/lpe-storage/src/mapi_contacts/record_contact_change_in_tx.md)
- [set_created_contact_modseq_in_tx](../../../../functions/crates/lpe-storage/src/mapi_contacts/set_created_contact_modseq_in_tx.md)
- [contact_affected_principals_in_tx](../../../../functions/crates/lpe-storage/src/mapi_contacts/contact_affected_principals_in_tx.md)

# Imports

- `std::collections::{BTreeMap, HashSet}`
- `anyhow::{anyhow, bail, Result}`
- `serde_json::Value`
- `sqlx::{Postgres, Row}`
- `uuid::Uuid`
- `crate::mapi_events::{
    mapi_change_key, mapi_store_id, merge_predecessor_change_list,
    FIRST_DYNAMIC_MAPI_GLOBAL_COUNTER, FIRST_RESERVED_HIGH_GLOBAL_COUNTER, MAX_MAPI_GLOBAL_COUNTER,
}`
- `crate::mapi_store_identity::{
    allocate_mapi_store_global_counter_in_tx, ensure_mapi_mailbox_replica_in_tx,
    ensure_mapi_store_identity_in_tx,
}`
- `crate::workspace::{
    contact_array_json, contact_emails_json, contact_phones_json, contact_primary_email,
    contact_source_payload_json,
}`
- `crate::{
    AccessibleContact, CanonicalChangeCategory, ContactNameFields, ContactSourceFields, Storage,
    UpsertClientContactInput, DEFAULT_CONTACT_BOOK_ROLE, IM_CONTACT_LIST_COLLECTION_ID,
    IM_CONTACT_LIST_ROLE, QUICK_CONTACTS_COLLECTION_ID, QUICK_CONTACTS_ROLE,
    SUGGESTED_CONTACTS_COLLECTION_ID, SUGGESTED_CONTACTS_ROLE,
}`
- `super::*`

# Member of

- [lpe-storage](../../../../packages/crates/lpe-storage.md)