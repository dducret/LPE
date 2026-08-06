---
type: Rust Function
title: mapi_private_logon_rops
resource: crates/lpe-exchange/src/tests/mod.rs#L12563-L12572
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_get_local_replica_ids_returns_replica_guid
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_get_local_replica_ids_distinguishes_null_and_non_logon_handles
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_get_local_replica_ids_returns_documented_failures
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/local_replica_ids/mapi_over_http_get_local_replica_ids_reserves_full_outlook_range_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_online_associated_config_create_is_atomic_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_common_views_online_create_ignores_client_source_key_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_online_common_views_wlink_accepts_later_ics_update_without_local_reservation
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_existing_common_views_wlink_stages_until_atomic_save_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_existing_common_views_wlink_entry_id_replacement_is_staged_until_atomic_save_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_common_views_non_wlink_fai_import_round_trips_durable_ics_identity_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_set_local_replica_midset_deleted_persists_folder_scoped_ranges
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_import_deletes_retry_ignores_online_unreserved_common_views_wlink
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_import_deletes_tombstones_reserved_unknown_common_views_wlink
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_import_deletes_prevalidates_common_views_batch_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_inbox_message_list_settings_import_preserves_outlook_system_properties_after_postgresql_reconnect
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/wlink_properties/mapi_over_http_wlink_client_properties_round_trip_postgresql_table_and_ics_after_reload
---

# Signature

`fn mapi_private_logon_rops(recipient: &str) -> Vec<u8>`

# Called by

- [mapi_over_http_get_local_replica_ids_returns_replica_guid](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_get_local_replica_ids_returns_replica_guid.md)
- [mapi_over_http_get_local_replica_ids_distinguishes_null_and_non_logon_handles](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_get_local_replica_ids_distinguishes_null_and_non_logon_handles.md)
- [mapi_over_http_get_local_replica_ids_returns_documented_failures](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_get_local_replica_ids_returns_documented_failures.md)
- [mapi_over_http_get_local_replica_ids_reserves_full_outlook_range_in_postgresql](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/local_replica_ids/mapi_over_http_get_local_replica_ids_reserves_full_outlook_range_in_postgresql.md)
- [mapi_over_http_online_associated_config_create_is_atomic_in_postgresql](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_online_associated_config_create_is_atomic_in_postgresql.md)
- [mapi_over_http_common_views_online_create_ignores_client_source_key_in_postgresql](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_common_views_online_create_ignores_client_source_key_in_postgresql.md)
- [mapi_over_http_online_common_views_wlink_accepts_later_ics_update_without_local_reservation](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_online_common_views_wlink_accepts_later_ics_update_without_local_reservation.md)
- [mapi_over_http_existing_common_views_wlink_stages_until_atomic_save_in_postgresql](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_existing_common_views_wlink_stages_until_atomic_save_in_postgresql.md)
- [mapi_over_http_existing_common_views_wlink_entry_id_replacement_is_staged_until_atomic_save_in_postgresql](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_existing_common_views_wlink_entry_id_replacement_is_staged_until_atomic_save_in_postgresql.md)
- [mapi_over_http_common_views_non_wlink_fai_import_round_trips_durable_ics_identity_in_postgresql](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_common_views_non_wlink_fai_import_round_trips_durable_ics_identity_in_postgresql.md)
- [mapi_over_http_set_local_replica_midset_deleted_persists_folder_scoped_ranges](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_set_local_replica_midset_deleted_persists_folder_scoped_ranges.md)
- [mapi_over_http_import_deletes_retry_ignores_online_unreserved_common_views_wlink](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_import_deletes_retry_ignores_online_unreserved_common_views_wlink.md)
- [mapi_over_http_import_deletes_tombstones_reserved_unknown_common_views_wlink](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_import_deletes_tombstones_reserved_unknown_common_views_wlink.md)
- [mapi_over_http_import_deletes_prevalidates_common_views_batch_in_postgresql](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_import_deletes_prevalidates_common_views_batch_in_postgresql.md)
- [mapi_over_http_inbox_message_list_settings_import_preserves_outlook_system_properties_after_postgresql_reconnect](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_inbox_message_list_settings_import_preserves_outlook_system_properties_after_postgresql_reconnect.md)
- [mapi_over_http_wlink_client_properties_round_trip_postgresql_table_and_ics_after_reload](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/wlink_properties/mapi_over_http_wlink_client_properties_round_trip_postgresql_table_and_ics_after_reload.md)