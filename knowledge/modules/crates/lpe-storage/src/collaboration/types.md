---
type: Rust Module
title: types
resource: crates/lpe-storage/src/collaboration/types.rs#L1-L736
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-bail-result
  - external/serde-deserialize-serialize
  - external/serde-json-value
  - external/sha2-digest-sha256
  - external/uuid-uuid
  - external/crate-collaborationcollectionrow-collaborationgrantrow-default-collection-id-default-contact-book-role-im-contact-list-collection-id-im-contact-list-role-quick-contacts-collection-id-quick-contacts-role-suggested-contacts-collection-id-suggested-contacts-role
  - external/super
  member_of:
  - packages/crates/lpe-storage
---

# Contains

- [CollaborationResourceKind](../../../../../classes/crates/lpe-storage/src/collaboration/types/CollaborationResourceKind.md)
- [as_str](../../../../../functions/crates/lpe-storage/src/collaboration/types/CollaborationResourceKind/as_str.md)
- [collection_label](../../../../../functions/crates/lpe-storage/src/collaboration/types/CollaborationResourceKind/collection_label.md)
- [CollaborationRights](../../../../../classes/crates/lpe-storage/src/collaboration/types/CollaborationRights.md)
- [CollaborationCollection](../../../../../classes/crates/lpe-storage/src/collaboration/types/CollaborationCollection.md)
- [ContactNameFields](../../../../../classes/crates/lpe-storage/src/collaboration/types/ContactNameFields.md)
- [default](../../../../../functions/crates/lpe-storage/src/collaboration/types/ContactNameFields/default/default.md)
- [ContactSourceFields](../../../../../classes/crates/lpe-storage/src/collaboration/types/ContactSourceFields.md)
- [default](../../../../../functions/crates/lpe-storage/src/collaboration/types/ContactSourceFields/default/default.md)
- [AccessibleContact](../../../../../classes/crates/lpe-storage/src/collaboration/types/AccessibleContact.md)
- [default](../../../../../functions/crates/lpe-storage/src/collaboration/types/AccessibleContact/default/default.md)
- [primary_email](../../../../../functions/crates/lpe-storage/src/collaboration/types/AccessibleContact/primary_email.md)
- [primary_phone](../../../../../functions/crates/lpe-storage/src/collaboration/types/AccessibleContact/primary_phone.md)
- [display_name](../../../../../functions/crates/lpe-storage/src/collaboration/types/AccessibleContact/display_name.md)
- [AccessibleEvent](../../../../../classes/crates/lpe-storage/src/collaboration/types/AccessibleEvent.md)
- [MapiEventIdentityMove](../../../../../classes/crates/lpe-storage/src/collaboration/types/MapiEventIdentityMove.md)
- [MapiEventImportedMoveIdentity](../../../../../classes/crates/lpe-storage/src/collaboration/types/MapiEventImportedMoveIdentity.md)
- [MoveAccessibleEventToDeletedItemsResult](../../../../../classes/crates/lpe-storage/src/collaboration/types/MoveAccessibleEventToDeletedItemsResult.md)
- [CollaborationGrant](../../../../../classes/crates/lpe-storage/src/collaboration/types/CollaborationGrant.md)
- [FreeBusyBlock](../../../../../classes/crates/lpe-storage/src/collaboration/types/FreeBusyBlock.md)
- [DelegateAccessObject](../../../../../classes/crates/lpe-storage/src/collaboration/types/DelegateAccessObject.md)
- [DelegateFreeBusyMessageObject](../../../../../classes/crates/lpe-storage/src/collaboration/types/DelegateFreeBusyMessageObject.md)
- [CollaborationGrantInput](../../../../../classes/crates/lpe-storage/src/collaboration/types/CollaborationGrantInput.md)
- [validate_collaboration_rights](../../../../../functions/crates/lpe-storage/src/collaboration/types/validate_collaboration_rights.md)
- [collection_id_for_owner](../../../../../functions/crates/lpe-storage/src/collaboration/types/collection_id_for_owner.md)
- [calendar_collection_id_for_event](../../../../../functions/crates/lpe-storage/src/collaboration/types/calendar_collection_id_for_event.md)
- [contact_book_role_for_collection_id](../../../../../functions/crates/lpe-storage/src/collaboration/types/contact_book_role_for_collection_id.md)
- [shared_collection_id](../../../../../functions/crates/lpe-storage/src/collaboration/types/shared_collection_id.md)
- [shared_collection_id_for_row](../../../../../functions/crates/lpe-storage/src/collaboration/types/shared_collection_id_for_row.md)
- [shared_collection_display_name](../../../../../functions/crates/lpe-storage/src/collaboration/types/shared_collection_display_name.md)
- [map_collaboration_grant](../../../../../functions/crates/lpe-storage/src/collaboration/types/map_collaboration_grant.md)
- [merge_free_busy_rows](../../../../../functions/crates/lpe-storage/src/collaboration/types/merge_free_busy_rows.md)
- [free_busy_status](../../../../../functions/crates/lpe-storage/src/collaboration/types/free_busy_status.md)
- [DelegateFreeBusyProjection](../../../../../classes/crates/lpe-storage/src/collaboration/types/DelegateFreeBusyProjection.md)
- [delegate_freebusy_projections](../../../../../functions/crates/lpe-storage/src/collaboration/types/delegate_freebusy_projections.md)
- [delegate_freebusy_message_objects](../../../../../functions/crates/lpe-storage/src/collaboration/types/delegate_freebusy_message_objects.md)
- [delegate_freebusy_projection_updated_at](../../../../../functions/crates/lpe-storage/src/collaboration/types/delegate_freebusy_projection_updated_at.md)
- [stable_delegate_freebusy_id](../../../../../functions/crates/lpe-storage/src/collaboration/types/stable_delegate_freebusy_id.md)
- [free_busy_rows_merge_adjacent_matching_states](../../../../../functions/crates/lpe-storage/src/collaboration/types/free_busy_rows_merge_adjacent_matching_states.md)
- [free_busy_without_calendar_access_hides_tentative_detail](../../../../../functions/crates/lpe-storage/src/collaboration/types/free_busy_without_calendar_access_hides_tentative_detail.md)
- [free_busy_cancelled_rows_stay_free_without_calendar_access](../../../../../functions/crates/lpe-storage/src/collaboration/types/free_busy_cancelled_rows_stay_free_without_calendar_access.md)
- [delegate_freebusy_projection_does_not_create_empty_placeholder](../../../../../functions/crates/lpe-storage/src/collaboration/types/delegate_freebusy_projection_does_not_create_empty_placeholder.md)
- [delegate_freebusy_projection_uses_only_canonical_delegate_and_blocks](../../../../../functions/crates/lpe-storage/src/collaboration/types/delegate_freebusy_projection_uses_only_canonical_delegate_and_blocks.md)
- [delegate_freebusy_message_objects_use_interval_commit_time_without_store_state](../../../../../functions/crates/lpe-storage/src/collaboration/types/delegate_freebusy_message_objects_use_interval_commit_time_without_store_state.md)

# Imports

- `anyhow::{bail, Result}`
- `serde::{Deserialize, Serialize}`
- `serde_json::Value`
- `sha2::{Digest, Sha256}`
- `uuid::Uuid`
- `crate::{
    CollaborationCollectionRow, CollaborationGrantRow, DEFAULT_COLLECTION_ID,
    DEFAULT_CONTACT_BOOK_ROLE, IM_CONTACT_LIST_COLLECTION_ID, IM_CONTACT_LIST_ROLE,
    QUICK_CONTACTS_COLLECTION_ID, QUICK_CONTACTS_ROLE, SUGGESTED_CONTACTS_COLLECTION_ID,
    SUGGESTED_CONTACTS_ROLE,
}`
- `super::*`

# Member of

- [lpe-storage](../../../../../packages/crates/lpe-storage.md)