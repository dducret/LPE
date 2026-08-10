---
type: Rust Module
title: mapi_events
resource: crates/lpe-storage/src/mapi_events.rs#L1-L1533
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/std-collections-btreemap-hashset
  - external/anyhow-anyhow-bail-result
  - external/sqlx-postgres-row
  - external/uuid-uuid
  - external/crate-mapi-store-identity-allocate-mapi-store-global-counter-in-tx-ensure-mapi-mailbox-replica-in-tx-mapi-first-global-counter-mapi-first-reserved-high-global-counter-mapi-max-global-counter-accessibleevent-calendareventattachment-canonicalchangecategory-collaborationrights-mapieventattachmentchanges-storage-upsertclienteventinput
  - external/imported-identity-allocate-mapi-event-identity-in-tx-validate-imported-identity
  - external/super
  member_of:
  - packages/crates/lpe-storage
---

# Contains

- [MapiEventReminderPatch](../../../../classes/crates/lpe-storage/src/mapi_events/MapiEventReminderPatch.md)
- [MapiEventCustomPropertyValue](../../../../classes/crates/lpe-storage/src/mapi_events/MapiEventCustomPropertyValue.md)
- [MapiEventCommitInput](../../../../classes/crates/lpe-storage/src/mapi_events/MapiEventCommitInput.md)
- [MapiEventCreateInput](../../../../classes/crates/lpe-storage/src/mapi_events/MapiEventCreateInput.md)
- [MapiEventImportedIdentity](../../../../classes/crates/lpe-storage/src/mapi_events/MapiEventImportedIdentity.md)
- [MapiEventReminderState](../../../../classes/crates/lpe-storage/src/mapi_events/MapiEventReminderState.md)
- [MapiEventVersion](../../../../classes/crates/lpe-storage/src/mapi_events/MapiEventVersion.md)
- [MapiEventCommitSuccess](../../../../classes/crates/lpe-storage/src/mapi_events/MapiEventCommitSuccess.md)
- [MapiEventCreateResult](../../../../classes/crates/lpe-storage/src/mapi_events/MapiEventCreateResult.md)
- [MapiEventCommitOutcome](../../../../classes/crates/lpe-storage/src/mapi_events/MapiEventCommitOutcome.md)
- [EventIdentityVersion](../../../../classes/crates/lpe-storage/src/mapi_events/EventIdentityVersion.md)
- [move_calendar_events_to_collection_in_tx](../../../../functions/crates/lpe-storage/src/mapi_events/Storage/move_calendar_events_to_collection_in_tx.md)
- [create_mapi_event](../../../../functions/crates/lpe-storage/src/mapi_events/Storage/create_mapi_event.md)
- [fetch_mapi_event_versions](../../../../functions/crates/lpe-storage/src/mapi_events/Storage/fetch_mapi_event_versions.md)
- [commit_mapi_event_update](../../../../functions/crates/lpe-storage/src/mapi_events/Storage/commit_mapi_event_update.md)
- [advance_calendar_event_version_in_tx](../../../../functions/crates/lpe-storage/src/mapi_events/Storage/advance_calendar_event_version_in_tx.md)
- [advance_mapi_event_version_for_lifecycle_in_tx](../../../../functions/crates/lpe-storage/src/mapi_events/Storage/advance_mapi_event_version_for_lifecycle_in_tx.md)
- [validate_mapi_event_create_input](../../../../functions/crates/lpe-storage/src/mapi_events/validate_mapi_event_create_input.md)
- [validate_mapi_event_commit_input](../../../../functions/crates/lpe-storage/src/mapi_events/validate_mapi_event_commit_input.md)
- [mapi_event_identity_object_kind](../../../../functions/crates/lpe-storage/src/mapi_events/mapi_event_identity_object_kind.md)
- [validate_mapi_event_fields](../../../../functions/crates/lpe-storage/src/mapi_events/validate_mapi_event_fields.md)
- [validate_mapi_event_reminder](../../../../functions/crates/lpe-storage/src/mapi_events/validate_mapi_event_reminder.md)
- [validate_mapi_event_custom_properties](../../../../functions/crates/lpe-storage/src/mapi_events/validate_mapi_event_custom_properties.md)
- [update_mapi_event_core_in_tx](../../../../functions/crates/lpe-storage/src/mapi_events/update_mapi_event_core_in_tx.md)
- [update_mapi_event_reminder_in_tx](../../../../functions/crates/lpe-storage/src/mapi_events/update_mapi_event_reminder_in_tx.md)
- [reminder_patch_has_changes](../../../../functions/crates/lpe-storage/src/mapi_events/reminder_patch_has_changes.md)
- [apply_mapi_event_custom_properties_in_tx](../../../../functions/crates/lpe-storage/src/mapi_events/apply_mapi_event_custom_properties_in_tx.md)
- [set_created_mapi_event_modseq_in_tx](../../../../functions/crates/lpe-storage/src/mapi_events/set_created_mapi_event_modseq_in_tx.md)
- [fetch_created_accessible_event_in_tx](../../../../functions/crates/lpe-storage/src/mapi_events/fetch_created_accessible_event_in_tx.md)
- [fetch_mapi_event_reminder_state_in_tx](../../../../functions/crates/lpe-storage/src/mapi_events/fetch_mapi_event_reminder_state_in_tx.md)
- [mapi_store_id](../../../../functions/crates/lpe-storage/src/mapi_events/mapi_store_id.md)
- [rotate_active_mapi_event_identities_in_tx](../../../../functions/crates/lpe-storage/src/mapi_events/rotate_active_mapi_event_identities_in_tx.md)
- [rotate_mapi_event_identities_in_tx](../../../../functions/crates/lpe-storage/src/mapi_events/rotate_mapi_event_identities_in_tx.md)
- [calendar_event_affected_principals_in_tx](../../../../functions/crates/lpe-storage/src/mapi_events/Storage/calendar_event_affected_principals_in_tx.md)
- [fetch_event_timestamps_in_tx](../../../../functions/crates/lpe-storage/src/mapi_events/fetch_event_timestamps_in_tx.md)
- [mapi_event_version_from_row](../../../../functions/crates/lpe-storage/src/mapi_events/mapi_event_version_from_row.md)
- [mapi_change_key](../../../../functions/crates/lpe-storage/src/mapi_events/mapi_change_key.md)
- [merge_predecessor_change_list](../../../../functions/crates/lpe-storage/src/mapi_events/merge_predecessor_change_list.md)
- [parse_predecessor_change_list](../../../../functions/crates/lpe-storage/src/mapi_events/parse_predecessor_change_list.md)
- [split_xid](../../../../functions/crates/lpe-storage/src/mapi_events/split_xid.md)
- [serialize_predecessor_change_list](../../../../functions/crates/lpe-storage/src/mapi_events/serialize_predecessor_change_list.md)
- [pcl_merge_keeps_the_latest_xid_per_replica_and_sorts_replicas](../../../../functions/crates/lpe-storage/src/mapi_events/pcl_merge_keeps_the_latest_xid_per_replica_and_sorts_replicas.md)

# Imports

- `std::collections::{BTreeMap, HashSet}`
- `anyhow::{anyhow, bail, Result}`
- `sqlx::{Postgres, Row}`
- `uuid::Uuid`
- `crate::{
    mapi_store_identity::{
        allocate_mapi_store_global_counter_in_tx, ensure_mapi_mailbox_replica_in_tx,
        MAPI_FIRST_GLOBAL_COUNTER, MAPI_FIRST_RESERVED_HIGH_GLOBAL_COUNTER,
        MAPI_MAX_GLOBAL_COUNTER,
    },
    AccessibleEvent, CalendarEventAttachment, CanonicalChangeCategory, CollaborationRights,
    MapiEventAttachmentChanges, Storage, UpsertClientEventInput,
}`
- `imported_identity::{allocate_mapi_event_identity_in_tx, validate_imported_identity}`
- `super::*`

# Member of

- [lpe-storage](../../../../packages/crates/lpe-storage.md)