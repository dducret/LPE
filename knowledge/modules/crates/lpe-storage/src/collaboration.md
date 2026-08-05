---
type: Rust Module
title: collaboration
resource: crates/lpe-storage/src/collaboration.rs#L1-L1586
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-anyhow-bail-result
  - external/uuid-uuid
  - external/sqlx-postgres-row
  - external/crate-canonicalchangecategory-collaborationcollectionrow-storage-upsertclientcontactinput-upsertclienteventinput-default-collection-id-default-contact-book-role-default-task-list-role-im-contact-list-collection-id-im-contact-list-role-quick-contacts-collection-id-quick-contacts-role-suggested-contacts-collection-id-suggested-contacts-role
  - external/pub-crate-use-types-validate-collaboration-rights
  - external/pub-use-types-accessiblecontact-accessibleevent-collaborationcollection-collaborationgrant-collaborationgrantinput-collaborationresourcekind-collaborationrights-contactnamefields-contactsourcefields-delegateaccessobject-delegatefreebusymessageobject-freebusyblock-mapieventidentitymove-mapieventimportedmoveidentity-moveaccessibleeventtodeleteditemsresult
  - external/types-calendar-collection-id-for-event-collection-id-for-owner-contact-book-role-for-collection-id-delegate-freebusy-message-objects-merge-free-busy-rows-shared-collection-display-name-shared-collection-id-for-row
  - external/super
  member_of:
  - packages/crates/lpe-storage
---

# Contains

- [fetch_accessible_contact_collections](../../../../functions/crates/lpe-storage/src/collaboration/Storage/fetch_accessible_contact_collections.md)
- [fetch_accessible_calendar_collections](../../../../functions/crates/lpe-storage/src/collaboration/Storage/fetch_accessible_calendar_collections.md)
- [fetch_delegate_access_objects](../../../../functions/crates/lpe-storage/src/collaboration/Storage/fetch_delegate_access_objects.md)
- [fetch_free_busy_blocks](../../../../functions/crates/lpe-storage/src/collaboration/Storage/fetch_free_busy_blocks.md)
- [project_delegate_freebusy_messages](../../../../functions/crates/lpe-storage/src/collaboration/Storage/project_delegate_freebusy_messages.md)
- [fetch_delegate_freebusy_messages](../../../../functions/crates/lpe-storage/src/collaboration/Storage/fetch_delegate_freebusy_messages.md)
- [compute_delegate_freebusy_messages](../../../../functions/crates/lpe-storage/src/collaboration/Storage/compute_delegate_freebusy_messages.md)
- [create_accessible_calendar_collection](../../../../functions/crates/lpe-storage/src/collaboration/Storage/create_accessible_calendar_collection.md)
- [update_accessible_calendar_collection](../../../../functions/crates/lpe-storage/src/collaboration/Storage/update_accessible_calendar_collection.md)
- [delete_accessible_calendar_collection](../../../../functions/crates/lpe-storage/src/collaboration/Storage/delete_accessible_calendar_collection.md)
- [fetch_accessible_task_collections](../../../../functions/crates/lpe-storage/src/collaboration/Storage/fetch_accessible_task_collections.md)
- [fetch_accessible_task_list_collections](../../../../functions/crates/lpe-storage/src/collaboration/Storage/fetch_accessible_task_list_collections.md)
- [fetch_accessible_contacts](../../../../functions/crates/lpe-storage/src/collaboration/Storage/fetch_accessible_contacts.md)
- [fetch_accessible_contacts_by_ids](../../../../functions/crates/lpe-storage/src/collaboration/Storage/fetch_accessible_contacts_by_ids.md)
- [fetch_accessible_contacts_in_collection](../../../../functions/crates/lpe-storage/src/collaboration/Storage/fetch_accessible_contacts_in_collection.md)
- [create_accessible_contact](../../../../functions/crates/lpe-storage/src/collaboration/Storage/create_accessible_contact.md)
- [update_accessible_contact](../../../../functions/crates/lpe-storage/src/collaboration/Storage/update_accessible_contact.md)
- [delete_accessible_contact](../../../../functions/crates/lpe-storage/src/collaboration/Storage/delete_accessible_contact.md)
- [fetch_accessible_events](../../../../functions/crates/lpe-storage/src/collaboration/Storage/fetch_accessible_events.md)
- [fetch_accessible_events_by_ids](../../../../functions/crates/lpe-storage/src/collaboration/Storage/fetch_accessible_events_by_ids.md)
- [fetch_accessible_events_in_collection](../../../../functions/crates/lpe-storage/src/collaboration/Storage/fetch_accessible_events_in_collection.md)
- [create_accessible_event](../../../../functions/crates/lpe-storage/src/collaboration/Storage/create_accessible_event.md)
- [update_accessible_event](../../../../functions/crates/lpe-storage/src/collaboration/Storage/update_accessible_event.md)
- [update_accessible_event_reminder](../../../../functions/crates/lpe-storage/src/collaboration/Storage/update_accessible_event_reminder.md)
- [delete_accessible_event](../../../../functions/crates/lpe-storage/src/collaboration/Storage/delete_accessible_event.md)
- [fetch_accessible_collections](../../../../functions/crates/lpe-storage/src/collaboration/Storage/fetch_accessible_collections.md)
- [resolve_collection_access](../../../../functions/crates/lpe-storage/src/collaboration/Storage/resolve_collection_access.md)
- [fetch_accessible_contacts_internal](../../../../functions/crates/lpe-storage/src/collaboration/Storage/fetch_accessible_contacts_internal.md)
- [fetch_accessible_events_internal](../../../../functions/crates/lpe-storage/src/collaboration/Storage/fetch_accessible_events_internal.md)
- [ensure_default_contact_book_in_tx](../../../../functions/crates/lpe-storage/src/collaboration/Storage/ensure_default_contact_book_in_tx.md)
- [ensure_contact_book_in_tx](../../../../functions/crates/lpe-storage/src/collaboration/Storage/ensure_contact_book_in_tx.md)
- [ensure_default_calendar_in_tx](../../../../functions/crates/lpe-storage/src/collaboration/Storage/ensure_default_calendar_in_tx.md)
- [task_collection_id_for_list](../../../../functions/crates/lpe-storage/src/collaboration/task_collection_id_for_list.md)
- [default_task_list_uses_stable_default_collection_id](../../../../functions/crates/lpe-storage/src/collaboration/default_task_list_uses_stable_default_collection_id.md)

# Imports

- `anyhow::{anyhow, bail, Result}`
- `uuid::Uuid`
- `sqlx::{Postgres, Row}`
- `crate::{
    CanonicalChangeCategory, CollaborationCollectionRow, Storage, UpsertClientContactInput,
    UpsertClientEventInput, DEFAULT_COLLECTION_ID, DEFAULT_CONTACT_BOOK_ROLE,
    DEFAULT_TASK_LIST_ROLE, IM_CONTACT_LIST_COLLECTION_ID, IM_CONTACT_LIST_ROLE,
    QUICK_CONTACTS_COLLECTION_ID, QUICK_CONTACTS_ROLE, SUGGESTED_CONTACTS_COLLECTION_ID,
    SUGGESTED_CONTACTS_ROLE,
}`
- `pub(crate) use types::validate_collaboration_rights`
- `pub use types::{
    AccessibleContact, AccessibleEvent, CollaborationCollection, CollaborationGrant,
    CollaborationGrantInput, CollaborationResourceKind, CollaborationRights, ContactNameFields,
    ContactSourceFields, DelegateAccessObject, DelegateFreeBusyMessageObject, FreeBusyBlock,
    MapiEventIdentityMove, MapiEventImportedMoveIdentity, MoveAccessibleEventToDeletedItemsResult,
}`
- `types::{
    calendar_collection_id_for_event, collection_id_for_owner, contact_book_role_for_collection_id,
    delegate_freebusy_message_objects, merge_free_busy_rows, shared_collection_display_name,
    shared_collection_id_for_row,
}`
- `super::*`

# Member of

- [lpe-storage](../../../../packages/crates/lpe-storage.md)