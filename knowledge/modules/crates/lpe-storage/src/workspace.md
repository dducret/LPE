---
type: Rust Module
title: workspace
resource: crates/lpe-storage/src/workspace.rs#L1-L1590
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-bail-result
  - external/serde-deserialize-serialize
  - external/serde-json-value
  - external/sqlx-row
  - external/uuid-uuid
  - external/crate-attachments-normalize-email-canonicalchangecategory-clientattachment-clientattachmentrow-clientcontactrow-clienteventrow-clientmessagerow-clienttask-contactnamefields-contactsourcefields-storage
  - external/super-client-folder-json-text-matches-merge-contact-update-input-clientcontact-clientevent-contactsourcefields-upsertclientcontactinput-value
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
- [fetch_client_workspace](../../../../functions/crates/lpe-storage/src/workspace/Storage/fetch_client_workspace.md)
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
- [body_paragraphs](../../../../functions/crates/lpe-storage/src/workspace/body_paragraphs.md)
- [client_folder](../../../../functions/crates/lpe-storage/src/workspace/client_folder.md)
- [client_message_tags](../../../../functions/crates/lpe-storage/src/workspace/client_message_tags.md)
- [format_size](../../../../functions/crates/lpe-storage/src/workspace/format_size.md)
- [map_event](../../../../functions/crates/lpe-storage/src/workspace/map_event.md)
- [map_contact](../../../../functions/crates/lpe-storage/src/workspace/map_contact.md)
- [client_address_book_id_for_role](../../../../functions/crates/lpe-storage/src/workspace/client_address_book_id_for_role.md)
- [client_folder_preserves_trash_role](../../../../functions/crates/lpe-storage/src/workspace/client_folder_preserves_trash_role.md)
- [workspace_contact_and_event_json_use_client_camel_case_contracts](../../../../functions/crates/lpe-storage/src/workspace/workspace_contact_and_event_json_use_client_camel_case_contracts.md)
- [canonical_event_json_comparison_ignores_whitespace](../../../../functions/crates/lpe-storage/src/workspace/canonical_event_json_comparison_ignores_whitespace.md)
- [contact_update_merges_missing_rich_fields](../../../../functions/crates/lpe-storage/src/workspace/contact_update_merges_missing_rich_fields.md)
- [contact_update_can_clear_explicit_rich_fields](../../../../functions/crates/lpe-storage/src/workspace/contact_update_can_clear_explicit_rich_fields.md)

# Imports

- `anyhow::{bail, Result}`
- `serde::{Deserialize, Serialize}`
- `serde_json::Value`
- `sqlx::Row`
- `uuid::Uuid`
- `crate::{
    attachments, normalize_email, CanonicalChangeCategory, ClientAttachment, ClientAttachmentRow,
    ClientContactRow, ClientEventRow, ClientMessageRow, ClientTask, ContactNameFields,
    ContactSourceFields, Storage,
}`
- `super::{
        client_folder, json_text_matches, merge_contact_update_input, ClientContact, ClientEvent,
        ContactSourceFields, UpsertClientContactInput, Value,
    }`
- `serde_json::json`

# Member of

- [lpe-storage](../../../../packages/crates/lpe-storage.md)