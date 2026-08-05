---
type: Rust Function
title: collaboration_folder_identity_canonical_id_for_collection
resource: crates/lpe-exchange/src/mapi_store.rs#L1020-L1033
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/deterministic_collaboration_folder_uuid
  called_by:
  - functions/crates/lpe-exchange/src/mapi_store/mapi_collaboration_folder_id_for_collection
  - functions/crates/lpe-exchange/src/mapi_store/collaboration_folder_identity_canonical_id
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/ScopedCalendarIdentities/folder_id
  - functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_calendar_notification_folder_id
  - functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_calendar_notification_folder_identity_ids_from_row
  - functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/calendar_move_is_suppressed_without_a_distinct_old_message_id
---

# Signature

`pub(crate) fn collaboration_folder_identity_canonical_id_for_collection( kind: MapiCollaborationFolderKind, collection_id: &str, ) -> Option<Uuid>`

# Calls

- [deterministic_collaboration_folder_uuid](../../../../../functions/crates/lpe-exchange/src/mapi_store/deterministic_collaboration_folder_uuid.md)

# Called by

- [mapi_collaboration_folder_id_for_collection](../../../../../functions/crates/lpe-exchange/src/mapi_store/mapi_collaboration_folder_id_for_collection.md)
- [collaboration_folder_identity_canonical_id](../../../../../functions/crates/lpe-exchange/src/mapi_store/collaboration_folder_identity_canonical_id.md)
- [folder_id](../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/ScopedCalendarIdentities/folder_id.md)
- [mapi_calendar_notification_folder_id](../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_calendar_notification_folder_id.md)
- [mapi_calendar_notification_folder_identity_ids_from_row](../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_calendar_notification_folder_identity_ids_from_row.md)
- [calendar_move_is_suppressed_without_a_distinct_old_message_id](../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/calendar_move_is_suppressed_without_a_distinct_old_message_id.md)