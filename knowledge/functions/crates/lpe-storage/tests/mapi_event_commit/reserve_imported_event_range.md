---
type: Rust Function
title: reserve_imported_event_range
resource: crates/lpe-storage/tests/mapi_event_commit.rs#L286-L321
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  - functions/tools/rca_outlook_connectivity_check/execute
  - functions/crates/lpe-storage/src/core/Storage/pool
  called_by:
  - functions/crates/lpe-storage/tests/mapi_event_commit/microsoft_oxcfxics_imported_event_keeps_client_xids_and_allocates_server_cn
  - functions/crates/lpe-storage/tests/mapi_event_commit/microsoft_oxcfxics_imported_calendar_move_is_atomic_and_keeps_destination_xids
  - functions/crates/lpe-storage/tests/mapi_event_commit/microsoft_oxcfxics_imported_deleted_event_update_keeps_identity_and_is_atomic
---

# Signature

`async fn reserve_imported_event_range( fixture: &EventFixture, first_global_counter: u64, last_global_counter: u64, ) -> Result<()>`

# Calls

- [query](../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [execute](../../../../../functions/tools/rca_outlook_connectivity_check/execute.md)
- [pool](../../../../../functions/crates/lpe-storage/src/core/Storage/pool.md)

# Called by

- [microsoft_oxcfxics_imported_event_keeps_client_xids_and_allocates_server_cn](../../../../../functions/crates/lpe-storage/tests/mapi_event_commit/microsoft_oxcfxics_imported_event_keeps_client_xids_and_allocates_server_cn.md)
- [microsoft_oxcfxics_imported_calendar_move_is_atomic_and_keeps_destination_xids](../../../../../functions/crates/lpe-storage/tests/mapi_event_commit/microsoft_oxcfxics_imported_calendar_move_is_atomic_and_keeps_destination_xids.md)
- [microsoft_oxcfxics_imported_deleted_event_update_keeps_identity_and_is_atomic](../../../../../functions/crates/lpe-storage/tests/mapi_event_commit/microsoft_oxcfxics_imported_deleted_event_update_keeps_identity_and_is_atomic.md)