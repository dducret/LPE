---
type: Rust Module
title: workspace
resource: crates/lpe-storage/src/workspace.rs#L1-L1392
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-result-bail
  - external/serde-deserialize-serialize
  - external/serde-json-value
  - external/sqlx-row
  - external/std-collections-btreemap
  - external/uuid-uuid
  - external/crate-canonicalchangecategory-clientattachment-clientcontactrow-clienteventrow-clienttask-collaborationcollection-contactnamefields-contactsourcefields-storage-normalize-email
  - external/super-clientcontact-clientevent-contactsourcefields-upsertclientcontactinput-value-client-folder-json-text-matches-merge-contact-update-input
  - external/serde-json-json
  member_of:
  - packages/crates/lpe-storage
---

# Contains

- [ClientWorkspace](../../../../classes/crates/lpe-storage/src/workspace/ClientWorkspace.md)
- [ClientMessage](../../../../classes/crates/lpe-storage/src/workspace/ClientMessage.md)
- [ClientEvent](../../../../classes/crates/lpe-storage/src/workspace/ClientEvent.md)
- [ClientContact](../../../../classes/crates/lpe-storage/src/workspace/ClientContact.md)
- [default](../../../../functions/crates/lpe-storage/src/workspace/ClientContact/default/default.md)
- [primary_email](../../../../functions/crates/lpe-storage/src/workspace/ClientContact/primary_email.md)
- [primary_phone](../../../../functions/crates/lpe-storage/src/workspace/ClientContact/primary_phone.md)
- [display_name](../../../../functions/crates/lpe-storage/src/workspace/ClientContact/display_name.md)
- [UpsertClientContactInput](../../../../classes/crates/lpe-storage/src/workspace/UpsertClientContactInput.md)
- [RecipientSuggestion](../../../../classes/crates/lpe-storage/src/workspace/RecipientSuggestion.md)
- [UpsertClientEventInput](../../../../classes/crates/lpe-storage/src/workspace/UpsertClientEventInput.md)
- [upsert_client_contact](../../../../functions/crates/lpe-storage/src/workspace/Storage/upsert_client_contact.md)
- [upsert_client_contact_in_book_role](../../../../functions/crates/lpe-storage/src/workspace/Storage/upsert_client_contact_in_book_role.md)
- [upsert_client_event](../../../../functions/crates/lpe-storage/src/workspace/Storage/upsert_client_event.md)
- [upsert_client_event_in_calendar](../../../../functions/crates/lpe-storage/src/workspace/Storage/upsert_client_event_in_calendar.md)
- [fetch_client_events](../../../../functions/crates/lpe-storage/src/workspace/Storage/fetch_client_events.md)
- [fetch_client_events_by_ids](../../../../functions/crates/lpe-storage/src/workspace/Storage/fetch_client_events_by_ids.md)
- [fetch_client_contacts](../../../../functions/crates/lpe-storage/src/workspace/Storage/fetch_client_contacts.md)
- [fetch_client_contacts_by_ids](../../../../functions/crates/lpe-storage/src/workspace/Storage/fetch_client_contacts_by_ids.md)
- [query_recipient_suggestions](../../../../functions/crates/lpe-storage/src/workspace/Storage/query_recipient_suggestions.md)
- [dismiss_recipient_suggestion](../../../../functions/crates/lpe-storage/src/workspace/Storage/dismiss_recipient_suggestion.md)
- [contact_emails_json](../../../../functions/crates/lpe-storage/src/workspace/contact_emails_json.md)
- [contact_phones_json](../../../../functions/crates/lpe-storage/src/workspace/contact_phones_json.md)
- [contact_array_json](../../../../functions/crates/lpe-storage/src/workspace/contact_array_json.md)
- [contact_source_payload_json](../../../../functions/crates/lpe-storage/src/workspace/contact_source_payload_json.md)
- [contact_primary_email](../../../../functions/crates/lpe-storage/src/workspace/contact_primary_email.md)
- [contact_update_is_unchanged](../../../../functions/crates/lpe-storage/src/workspace/contact_update_is_unchanged.md)
- [event_update_is_unchanged](../../../../functions/crates/lpe-storage/src/workspace/event_update_is_unchanged.md)
- [json_text_matches](../../../../functions/crates/lpe-storage/src/workspace/json_text_matches.md)
- [merge_contact_update_input](../../../../functions/crates/lpe-storage/src/workspace/merge_contact_update_input.md)
- [contact_json_with_primary_value](../../../../functions/crates/lpe-storage/src/workspace/contact_json_with_primary_value.md)
- [client_folder](../../../../functions/crates/lpe-storage/src/workspace/client_folder.md)
- [map_event](../../../../functions/crates/lpe-storage/src/workspace/map_event.md)
- [map_contact](../../../../functions/crates/lpe-storage/src/workspace/map_contact.md)
- [client_address_book_id_for_role](../../../../functions/crates/lpe-storage/src/workspace/client_address_book_id_for_role.md)
- [client_folder_preserves_trash_role](../../../../functions/crates/lpe-storage/src/workspace/client_folder_preserves_trash_role.md)
- [workspace_contact_and_event_json_use_client_camel_case_contracts](../../../../functions/crates/lpe-storage/src/workspace/workspace_contact_and_event_json_use_client_camel_case_contracts.md)
- [canonical_event_json_comparison_ignores_whitespace](../../../../functions/crates/lpe-storage/src/workspace/canonical_event_json_comparison_ignores_whitespace.md)
- [contact_update_merges_missing_rich_fields](../../../../functions/crates/lpe-storage/src/workspace/contact_update_merges_missing_rich_fields.md)
- [contact_update_can_clear_explicit_rich_fields](../../../../functions/crates/lpe-storage/src/workspace/contact_update_can_clear_explicit_rich_fields.md)

# Imports

- `anyhow::{Result, bail}`
- `serde::{Deserialize, Serialize}`
- `serde_json::Value`
- `sqlx::Row`
- `std::collections::BTreeMap`
- `uuid::Uuid`
- `crate::{
    CanonicalChangeCategory, ClientAttachment, ClientContactRow, ClientEventRow, ClientTask,
    CollaborationCollection, ContactNameFields, ContactSourceFields, Storage, normalize_email,
}`
- `super::{
        ClientContact, ClientEvent, ContactSourceFields, UpsertClientContactInput, Value,
        client_folder, json_text_matches, merge_contact_update_input,
    }`
- `serde_json::json`

# Member of

- [lpe-storage](../../../../packages/crates/lpe-storage.md)