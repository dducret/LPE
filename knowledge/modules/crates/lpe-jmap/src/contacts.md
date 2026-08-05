---
type: Rust Module
title: contacts
resource: crates/lpe-jmap/src/contacts.rs#L1-L1051
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-anyhow-bail-result
  - external/lpe-domain-normalization
  - external/lpe-storage-accessiblecontact-authenticatedaccount-collaborationcollection-contactnamefields-contactsourcefields-recipientsuggestion-upsertclientcontactinput
  - external/serde-json-json-map-value
  - external/std-collections-hashmap-hashset
  - external/uuid-uuid
  - external/crate-convert-apply-jmap-property-patch-has-jmap-property-patch-insert-if-error-set-error-parse-parse-uuid-parse-uuid-list-protocol-addressbookgetarguments-addressbookqueryarguments-changesarguments-contactcardgetarguments-contactcardqueryarguments-contactcardqueryfilter-contactcardsetarguments-entityquerysort-querychangesarguments-recipientsuggestionqueryarguments-state-query-changes-response-query-position-stateentry-validation-validate-contact-filter-validate-entity-sort-jmapservice-default-get-limit-max-query-limit
  member_of:
  - packages/crates/lpe-jmap
---

# Contains

- [handle_address_book_get](../../../../functions/crates/lpe-jmap/src/contacts/JmapService/handle_address_book_get.md)
- [handle_address_book_query](../../../../functions/crates/lpe-jmap/src/contacts/JmapService/handle_address_book_query.md)
- [handle_address_book_query_changes](../../../../functions/crates/lpe-jmap/src/contacts/JmapService/handle_address_book_query_changes.md)
- [handle_address_book_changes](../../../../functions/crates/lpe-jmap/src/contacts/JmapService/handle_address_book_changes.md)
- [handle_contact_get](../../../../functions/crates/lpe-jmap/src/contacts/JmapService/handle_contact_get.md)
- [handle_contact_query](../../../../functions/crates/lpe-jmap/src/contacts/JmapService/handle_contact_query.md)
- [handle_contact_query_changes](../../../../functions/crates/lpe-jmap/src/contacts/JmapService/handle_contact_query_changes.md)
- [handle_contact_changes](../../../../functions/crates/lpe-jmap/src/contacts/JmapService/handle_contact_changes.md)
- [handle_contact_set](../../../../functions/crates/lpe-jmap/src/contacts/JmapService/handle_contact_set.md)
- [handle_recipient_suggestion_query](../../../../functions/crates/lpe-jmap/src/contacts/JmapService/handle_recipient_suggestion_query.md)
- [contact_update_input](../../../../functions/crates/lpe-jmap/src/contacts/JmapService/contact_update_input.md)
- [address_book_properties](../../../../functions/crates/lpe-jmap/src/contacts/address_book_properties.md)
- [address_book_to_value](../../../../functions/crates/lpe-jmap/src/contacts/address_book_to_value.md)
- [contact_properties](../../../../functions/crates/lpe-jmap/src/contacts/contact_properties.md)
- [contact_to_value](../../../../functions/crates/lpe-jmap/src/contacts/contact_to_value.md)
- [recipient_suggestion_to_value](../../../../functions/crates/lpe-jmap/src/contacts/recipient_suggestion_to_value.md)
- [insert_non_empty_object](../../../../functions/crates/lpe-jmap/src/contacts/insert_non_empty_object.md)
- [contact_array_to_named_object](../../../../functions/crates/lpe-jmap/src/contacts/contact_array_to_named_object.md)
- [contact_matches_filter](../../../../functions/crates/lpe-jmap/src/contacts/contact_matches_filter.md)
- [collection_sort_key](../../../../functions/crates/lpe-jmap/src/contacts/collection_sort_key.md)
- [serialize_entity_query_sort](../../../../functions/crates/lpe-jmap/src/contacts/serialize_entity_query_sort.md)
- [reject_collection_query_constraints](../../../../functions/crates/lpe-jmap/src/contacts/reject_collection_query_constraints.md)
- [parse_contact_input](../../../../functions/crates/lpe-jmap/src/contacts/parse_contact_input.md)
- [reject_unknown_contact_properties](../../../../functions/crates/lpe-jmap/src/contacts/reject_unknown_contact_properties.md)
- [validate_address_book_ids](../../../../functions/crates/lpe-jmap/src/contacts/validate_address_book_ids.md)
- [parse_contact_name](../../../../functions/crates/lpe-jmap/src/contacts/parse_contact_name.md)
- [parse_contact_name_fields](../../../../functions/crates/lpe-jmap/src/contacts/parse_contact_name_fields.md)
- [contact_object_string](../../../../functions/crates/lpe-jmap/src/contacts/contact_object_string.md)
- [parse_contact_email](../../../../functions/crates/lpe-jmap/src/contacts/parse_contact_email.md)
- [parse_contact_phone](../../../../functions/crates/lpe-jmap/src/contacts/parse_contact_phone.md)
- [parse_contact_organization](../../../../functions/crates/lpe-jmap/src/contacts/parse_contact_organization.md)
- [parse_contact_title](../../../../functions/crates/lpe-jmap/src/contacts/parse_contact_title.md)
- [parse_contact_note](../../../../functions/crates/lpe-jmap/src/contacts/parse_contact_note.md)
- [parse_contact_organization_name](../../../../functions/crates/lpe-jmap/src/contacts/parse_contact_organization_name.md)
- [parse_contact_job_title](../../../../functions/crates/lpe-jmap/src/contacts/parse_contact_job_title.md)
- [parse_contact_property_string](../../../../functions/crates/lpe-jmap/src/contacts/parse_contact_property_string.md)
- [parse_contact_property_array](../../../../functions/crates/lpe-jmap/src/contacts/parse_contact_property_array.md)
- [parse_contact_property_entry](../../../../functions/crates/lpe-jmap/src/contacts/parse_contact_property_entry.md)

# Imports

- `anyhow::{anyhow, bail, Result}`
- `lpe_domain::normalization`
- `lpe_storage::{
    AccessibleContact, AuthenticatedAccount, CollaborationCollection, ContactNameFields,
    ContactSourceFields, RecipientSuggestion, UpsertClientContactInput,
}`
- `serde_json::{json, Map, Value}`
- `std::collections::{HashMap, HashSet}`
- `uuid::Uuid`
- `crate::{
    convert::{apply_jmap_property_patch, has_jmap_property_patch, insert_if},
    error::set_error,
    parse::{parse_uuid, parse_uuid_list},
    protocol::{
        AddressBookGetArguments, AddressBookQueryArguments, ChangesArguments,
        ContactCardGetArguments, ContactCardQueryArguments, ContactCardQueryFilter,
        ContactCardSetArguments, EntityQuerySort, QueryChangesArguments,
        RecipientSuggestionQueryArguments,
    },
    state::{query_changes_response, query_position, StateEntry},
    validation::{validate_contact_filter, validate_entity_sort},
    JmapService, DEFAULT_GET_LIMIT, MAX_QUERY_LIMIT,
}`

# Member of

- [lpe-jmap](../../../../packages/crates/lpe-jmap.md)