---
type: Rust Function
title: mapi_wire_id_bytes
resource: crates/lpe-exchange/src/tests/mod.rs#L15135-L15137
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_move_to_deleted_items_rekeys_and_projects_canonical_event
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_default_entry_id_converts_to_openable_folder_id
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_outlook_startup_calendar_folder_chain_uses_advertised_default_calendar
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_run_1903_delivers_read_state_change_as_rop_notify
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_depth_root_hierarchy_table_delivers_informative_folder_rows
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_replays_outlook_contact_sync_import_then_save
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/hierarchy/mapi_over_http_create_folder_advertised_special_folder_opens_existing_even_without_flag
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/hierarchy/mapi_over_http_create_folder_quick_step_settings_opens_advertised_special_folder
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/hierarchy/mapi_over_http_notification_wait_serializes_canonical_hierarchy_details
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_online_common_views_wlink_accepts_later_ics_update_without_local_reservation
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_common_views_import_classifies_non_wlink_fai_at_save
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_common_views_non_wlink_fai_import_round_trips_durable_ics_identity_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_common_views_fai_table_open_and_ics_share_canonical_identity
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_outlook_mail_favorite_import_without_group_properties_persists
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_outlook_common_views_ics_import_stages_wlinks_until_save
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_replays_outlook_calendar_sync_import_then_save
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_replays_outlook_calendar_move_then_modifies_deleted_event
---

# Signature

`fn mapi_wire_id_bytes(object_id: u64) -> [u8; 8]`

# Called by

- [mapi_over_http_calendar_move_to_deleted_items_rekeys_and_projects_canonical_event](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_move_to_deleted_items_rekeys_and_projects_canonical_event.md)
- [mapi_over_http_calendar_default_entry_id_converts_to_openable_folder_id](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_default_entry_id_converts_to_openable_folder_id.md)
- [mapi_over_http_outlook_startup_calendar_folder_chain_uses_advertised_default_calendar](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_outlook_startup_calendar_folder_chain_uses_advertised_default_calendar.md)
- [mapi_over_http_run_1903_delivers_read_state_change_as_rop_notify](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_run_1903_delivers_read_state_change_as_rop_notify.md)
- [mapi_over_http_depth_root_hierarchy_table_delivers_informative_folder_rows](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_depth_root_hierarchy_table_delivers_informative_folder_rows.md)
- [mapi_over_http_replays_outlook_contact_sync_import_then_save](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_replays_outlook_contact_sync_import_then_save.md)
- [mapi_over_http_create_folder_advertised_special_folder_opens_existing_even_without_flag](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/hierarchy/mapi_over_http_create_folder_advertised_special_folder_opens_existing_even_without_flag.md)
- [mapi_over_http_create_folder_quick_step_settings_opens_advertised_special_folder](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/hierarchy/mapi_over_http_create_folder_quick_step_settings_opens_advertised_special_folder.md)
- [mapi_over_http_notification_wait_serializes_canonical_hierarchy_details](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/hierarchy/mapi_over_http_notification_wait_serializes_canonical_hierarchy_details.md)
- [mapi_over_http_online_common_views_wlink_accepts_later_ics_update_without_local_reservation](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_online_common_views_wlink_accepts_later_ics_update_without_local_reservation.md)
- [mapi_over_http_common_views_import_classifies_non_wlink_fai_at_save](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_common_views_import_classifies_non_wlink_fai_at_save.md)
- [mapi_over_http_common_views_non_wlink_fai_import_round_trips_durable_ics_identity_in_postgresql](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_common_views_non_wlink_fai_import_round_trips_durable_ics_identity_in_postgresql.md)
- [mapi_over_http_common_views_fai_table_open_and_ics_share_canonical_identity](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_common_views_fai_table_open_and_ics_share_canonical_identity.md)
- [mapi_over_http_outlook_mail_favorite_import_without_group_properties_persists](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_outlook_mail_favorite_import_without_group_properties_persists.md)
- [mapi_over_http_outlook_common_views_ics_import_stages_wlinks_until_save](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_outlook_common_views_ics_import_stages_wlinks_until_save.md)
- [mapi_over_http_replays_outlook_calendar_sync_import_then_save](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_replays_outlook_calendar_sync_import_then_save.md)
- [mapi_over_http_replays_outlook_calendar_move_then_modifies_deleted_event](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_replays_outlook_calendar_move_then_modifies_deleted_event.md)