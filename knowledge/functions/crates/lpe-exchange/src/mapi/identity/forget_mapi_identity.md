---
type: Rust Function
title: forget_mapi_identity
resource: crates/lpe-exchange/src/mapi/identity.rs#L693-L700
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/current_mapi_request_identities
  - functions/crates/lpe-exchange/src/mapi/identity/MapiRequestIdentityScope/forget
  - functions/LPE-CT/web/app/smoke/test/MockClassList/remove
  called_by:
  - functions/crates/lpe-exchange/src/mapi/identity/owner_and_grantee_scopes_keep_hierarchy_folder_wire_ids_separate
  - functions/crates/lpe-exchange/src/mapi/identity/forgotten_mapi_identity_is_not_mapped
  - functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_store_for_access_plan
  - functions/crates/lpe-exchange/src/mapi_store/tests/event_lookup_rejects_another_principals_cached_mid
  - functions/crates/lpe-exchange/src/mapi_store/tests/exact_event_mid_wins_over_another_events_foreign_cached_alias
  - functions/crates/lpe-exchange/src/mapi_store/tests/snapshot_falls_back_when_custom_collaboration_identity_is_not_loaded
  - functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/calendar_notification_identity_never_falls_back_to_another_principal_cache_entry
---

# Signature

`pub(crate) fn forget_mapi_identity(canonical_id: &Uuid)`

# Calls

- [current_mapi_request_identities](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/current_mapi_request_identities.md)
- [forget](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiRequestIdentityScope/forget.md)
- [remove](../../../../../../functions/LPE-CT/web/app/smoke/test/MockClassList/remove.md)

# Called by

- [owner_and_grantee_scopes_keep_hierarchy_folder_wire_ids_separate](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/owner_and_grantee_scopes_keep_hierarchy_folder_wire_ids_separate.md)
- [forgotten_mapi_identity_is_not_mapped](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/forgotten_mapi_identity_is_not_mapped.md)
- [load_mapi_store_for_access_plan](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_store_for_access_plan.md)
- [event_lookup_rejects_another_principals_cached_mid](../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/event_lookup_rejects_another_principals_cached_mid.md)
- [exact_event_mid_wins_over_another_events_foreign_cached_alias](../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/exact_event_mid_wins_over_another_events_foreign_cached_alias.md)
- [snapshot_falls_back_when_custom_collaboration_identity_is_not_loaded](../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/snapshot_falls_back_when_custom_collaboration_identity_is_not_loaded.md)
- [calendar_notification_identity_never_falls_back_to_another_principal_cache_entry](../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/calendar_notification_identity_never_falls_back_to_another_principal_cache_entry.md)