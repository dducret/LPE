---
type: Rust Function
title: scoped_identity_event_input
resource: crates/lpe-exchange/src/tests/mapi_over_http/calendar_identity_scope.rs#L3-L31
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar_identity_scope/mapi_calendar_snapshot_identity_is_principal_scoped_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar_identity_scope/mapi_identity_repair_preserves_rotated_calendar_change_key
---

# Signature

`fn scoped_identity_event_input( account_id: Uuid, event_id: Uuid, collection_id: &str, ) -> UpsertClientEventInput`

# Called by

- [mapi_calendar_snapshot_identity_is_principal_scoped_in_postgresql](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar_identity_scope/mapi_calendar_snapshot_identity_is_principal_scoped_in_postgresql.md)
- [mapi_identity_repair_preserves_rotated_calendar_change_key](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar_identity_scope/mapi_identity_repair_preserves_rotated_calendar_change_key.md)