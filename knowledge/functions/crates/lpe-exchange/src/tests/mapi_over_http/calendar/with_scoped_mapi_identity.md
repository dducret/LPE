---
type: Rust Function
title: with_scoped_mapi_identity
resource: crates/lpe-exchange/src/tests/mapi_over_http/calendar.rs#L3-L15
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_identity_codec_for_test
  - functions/crates/lpe-core/src/sieve/Parser/expect
  - functions/crates/lpe-exchange/src/mapi/identity/with_current_mapi_identity_codec
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/with_default_scoped_mapi_identity
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_open_message_resolves_virtual_local_freebusy_without_folder_id
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_ipm_subtree_hierarchy_findrow_finds_calendar_by_entry_id
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_hierarchy_inbox_default_calendar_entry_id_uses_account_guid
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_hierarchy_synthetic_inbox_default_calendar_entry_id_uses_account_guid
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_hierarchy_find_row_default_calendar_entry_id_uses_account_guid
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_hierarchy_find_row_by_inbox_default_calendar_entry_id_matches_real_inbox
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_hierarchy_find_row_by_inbox_default_calendar_entry_id_matches_synthetic_inbox
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_outlook_startup_calendar_folder_chain_uses_advertised_default_calendar
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_ms_oxosfld_calendar_lookup_chain_opens_calendar_from_inbox
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_folder_open_projects_entry_id_identity
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_hierarchy_row_projects_entry_id_identity
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_custom_calendar_hierarchy_sync_projects_owner_entry_id_identity
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_get_properties_all_lists_entry_id_identity
---

# Signature

`async fn with_scoped_mapi_identity<T>( store: &FakeStore, account_id: Uuid, operation: impl FnOnce() -> T, ) -> T`

# Calls

- [load_mapi_identity_codec_for_test](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_identity_codec_for_test.md)
- [expect](../../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
- [with_current_mapi_identity_codec](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/with_current_mapi_identity_codec.md)

# Called by

- [with_default_scoped_mapi_identity](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/with_default_scoped_mapi_identity.md)
- [mapi_over_http_open_message_resolves_virtual_local_freebusy_without_folder_id](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_open_message_resolves_virtual_local_freebusy_without_folder_id.md)
- [mapi_over_http_ipm_subtree_hierarchy_findrow_finds_calendar_by_entry_id](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_ipm_subtree_hierarchy_findrow_finds_calendar_by_entry_id.md)
- [mapi_over_http_hierarchy_inbox_default_calendar_entry_id_uses_account_guid](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_hierarchy_inbox_default_calendar_entry_id_uses_account_guid.md)
- [mapi_over_http_hierarchy_synthetic_inbox_default_calendar_entry_id_uses_account_guid](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_hierarchy_synthetic_inbox_default_calendar_entry_id_uses_account_guid.md)
- [mapi_over_http_hierarchy_find_row_default_calendar_entry_id_uses_account_guid](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_hierarchy_find_row_default_calendar_entry_id_uses_account_guid.md)
- [mapi_over_http_hierarchy_find_row_by_inbox_default_calendar_entry_id_matches_real_inbox](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_hierarchy_find_row_by_inbox_default_calendar_entry_id_matches_real_inbox.md)
- [mapi_over_http_hierarchy_find_row_by_inbox_default_calendar_entry_id_matches_synthetic_inbox](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_hierarchy_find_row_by_inbox_default_calendar_entry_id_matches_synthetic_inbox.md)
- [mapi_over_http_outlook_startup_calendar_folder_chain_uses_advertised_default_calendar](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_outlook_startup_calendar_folder_chain_uses_advertised_default_calendar.md)
- [mapi_over_http_ms_oxosfld_calendar_lookup_chain_opens_calendar_from_inbox](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_ms_oxosfld_calendar_lookup_chain_opens_calendar_from_inbox.md)
- [mapi_over_http_calendar_folder_open_projects_entry_id_identity](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_folder_open_projects_entry_id_identity.md)
- [mapi_over_http_calendar_hierarchy_row_projects_entry_id_identity](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_hierarchy_row_projects_entry_id_identity.md)
- [mapi_over_http_custom_calendar_hierarchy_sync_projects_owner_entry_id_identity](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_custom_calendar_hierarchy_sync_projects_owner_entry_id_identity.md)
- [mapi_over_http_calendar_get_properties_all_lists_entry_id_identity](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_get_properties_all_lists_entry_id_identity.md)