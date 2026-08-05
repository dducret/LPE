---
type: Rust Function
title: change_key
resource: crates/lpe-storage/tests/mapi_event_commit.rs#L2908-L2913
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/tests/mapi_event_commit/event_fixture
  - functions/crates/lpe-storage/tests/mapi_event_commit/mapi_message_mutations_rotate_durable_mapi_version_without_rekeying_identity
  - functions/crates/lpe-storage/tests/mapi_event_commit/microsoft_oxcfxics_imported_event_keeps_client_xids_and_allocates_server_cn
  - functions/crates/lpe-storage/tests/mapi_event_commit/microsoft_oxcfxics_imported_calendar_move_is_atomic_and_keeps_destination_xids
  - functions/crates/lpe-storage/tests/mapi_event_commit/microsoft_oxcfxics_imported_deleted_event_update_keeps_identity_and_is_atomic
  - functions/crates/lpe-storage/tests/mapi_event_commit/calendar_event_move_to_deleted_items_preserves_canonical_content_and_rekeys_identity
---

# Signature

`fn change_key(global_counter: u64) -> Vec<u8>`

# Called by

- [event_fixture](../../../../../functions/crates/lpe-storage/tests/mapi_event_commit/event_fixture.md)
- [mapi_message_mutations_rotate_durable_mapi_version_without_rekeying_identity](../../../../../functions/crates/lpe-storage/tests/mapi_event_commit/mapi_message_mutations_rotate_durable_mapi_version_without_rekeying_identity.md)
- [microsoft_oxcfxics_imported_event_keeps_client_xids_and_allocates_server_cn](../../../../../functions/crates/lpe-storage/tests/mapi_event_commit/microsoft_oxcfxics_imported_event_keeps_client_xids_and_allocates_server_cn.md)
- [microsoft_oxcfxics_imported_calendar_move_is_atomic_and_keeps_destination_xids](../../../../../functions/crates/lpe-storage/tests/mapi_event_commit/microsoft_oxcfxics_imported_calendar_move_is_atomic_and_keeps_destination_xids.md)
- [microsoft_oxcfxics_imported_deleted_event_update_keeps_identity_and_is_atomic](../../../../../functions/crates/lpe-storage/tests/mapi_event_commit/microsoft_oxcfxics_imported_deleted_event_update_keeps_identity_and_is_atomic.md)
- [calendar_event_move_to_deleted_items_preserves_canonical_content_and_rekeys_identity](../../../../../functions/crates/lpe-storage/tests/mapi_event_commit/calendar_event_move_to_deleted_items_preserves_canonical_content_and_rekeys_identity.md)