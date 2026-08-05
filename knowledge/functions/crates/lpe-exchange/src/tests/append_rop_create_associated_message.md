---
type: Rust Function
title: append_rop_create_associated_message
resource: crates/lpe-exchange/src/tests/mod.rs#L15057-L15062
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/append_mapi_wire_id
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_microsoft_oxocfg_writing_view_definition_sequence_succeeds
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_common_views_create_group_header_and_link_persists_and_reloads
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_microsoft_oxocfg_same_target_wlinks_round_trip_distinctly
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_contact_link_copy_to_uses_message_content_root
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_outlook_contact_prefs_save_accepts_combined_force_flags
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_created_contact_link_config_accepts_outlook_marker_property
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_common_view_named_view_accepts_microsoft_descriptor_write_stream_batch
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_online_associated_config_create_is_atomic_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_conversation_action_fai_persists_and_moves_existing_conversation
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_common_views_create_associated_navigation_shortcut_persists
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_common_views_rejects_incomplete_wlink_without_synthetic_defaults
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_common_views_accepts_outlook_calendar_group_header_without_group_name
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_common_views_online_create_ignores_client_source_key_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_online_common_views_wlink_accepts_later_ics_update_without_local_reservation
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_common_views_keeps_identical_online_fai_messages_distinct
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_associated_message_uploads_do_not_create_visible_items
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_microsoft_oxocfg_configuration_examples_round_trip_fai
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/wlink_properties/mapi_over_http_wlink_client_properties_round_trip_postgresql_table_and_ics_after_reload
---

# Signature

`fn append_rop_create_associated_message(rops: &mut Vec<u8>, input: u8, output: u8, folder_id: u64)`

# Calls

- [append_mapi_wire_id](../../../../../functions/crates/lpe-exchange/src/tests/append_mapi_wire_id.md)
- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [mapi_over_http_microsoft_oxocfg_writing_view_definition_sequence_succeeds](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_microsoft_oxocfg_writing_view_definition_sequence_succeeds.md)
- [mapi_over_http_common_views_create_group_header_and_link_persists_and_reloads](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_common_views_create_group_header_and_link_persists_and_reloads.md)
- [mapi_over_http_microsoft_oxocfg_same_target_wlinks_round_trip_distinctly](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_microsoft_oxocfg_same_target_wlinks_round_trip_distinctly.md)
- [mapi_over_http_contact_link_copy_to_uses_message_content_root](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_contact_link_copy_to_uses_message_content_root.md)
- [mapi_over_http_outlook_contact_prefs_save_accepts_combined_force_flags](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_outlook_contact_prefs_save_accepts_combined_force_flags.md)
- [mapi_over_http_created_contact_link_config_accepts_outlook_marker_property](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_created_contact_link_config_accepts_outlook_marker_property.md)
- [mapi_over_http_common_view_named_view_accepts_microsoft_descriptor_write_stream_batch](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_common_view_named_view_accepts_microsoft_descriptor_write_stream_batch.md)
- [mapi_over_http_online_associated_config_create_is_atomic_in_postgresql](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_online_associated_config_create_is_atomic_in_postgresql.md)
- [mapi_over_http_conversation_action_fai_persists_and_moves_existing_conversation](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_conversation_action_fai_persists_and_moves_existing_conversation.md)
- [mapi_over_http_common_views_create_associated_navigation_shortcut_persists](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_common_views_create_associated_navigation_shortcut_persists.md)
- [mapi_over_http_common_views_rejects_incomplete_wlink_without_synthetic_defaults](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_common_views_rejects_incomplete_wlink_without_synthetic_defaults.md)
- [mapi_over_http_common_views_accepts_outlook_calendar_group_header_without_group_name](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_common_views_accepts_outlook_calendar_group_header_without_group_name.md)
- [mapi_over_http_common_views_online_create_ignores_client_source_key_in_postgresql](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_common_views_online_create_ignores_client_source_key_in_postgresql.md)
- [mapi_over_http_online_common_views_wlink_accepts_later_ics_update_without_local_reservation](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_online_common_views_wlink_accepts_later_ics_update_without_local_reservation.md)
- [mapi_over_http_common_views_keeps_identical_online_fai_messages_distinct](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_common_views_keeps_identical_online_fai_messages_distinct.md)
- [mapi_over_http_associated_message_uploads_do_not_create_visible_items](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_associated_message_uploads_do_not_create_visible_items.md)
- [mapi_over_http_microsoft_oxocfg_configuration_examples_round_trip_fai](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_microsoft_oxocfg_configuration_examples_round_trip_fai.md)
- [mapi_over_http_wlink_client_properties_round_trip_postgresql_table_and_ics_after_reload](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/wlink_properties/mapi_over_http_wlink_client_properties_round_trip_postgresql_table_and_ics_after_reload.md)