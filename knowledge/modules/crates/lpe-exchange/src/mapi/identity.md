---
type: Rust Module
title: identity
resource: crates/lpe-exchange/src/mapi/identity.rs#L1-L1627
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/super
  - external/crate-store-mapiidentityobjectkind-mapiidentityrecord-mapiidentityrequest
  - external/anyhow-anyhow-result
  - external/std-sync-arc
  member_of:
  - packages/crates/lpe-exchange
---

# Contains

- [with_current_mapi_identity_codec](../../../../../functions/crates/lpe-exchange/src/mapi/identity/with_current_mapi_identity_codec.md)
- [current_mapi_identity_codec](../../../../../functions/crates/lpe-exchange/src/mapi/identity/current_mapi_identity_codec.md)
- [current_mapi_request_identities](../../../../../functions/crates/lpe-exchange/src/mapi/identity/current_mapi_request_identities.md)
- [current_store_replica_guid](../../../../../functions/crates/lpe-exchange/src/mapi/identity/current_store_replica_guid.md)
- [durable_object_id](../../../../../functions/crates/lpe-exchange/src/mapi/identity/durable_object_id.md)
- [MapiIdentityMaterial](../../../../../classes/crates/lpe-exchange/src/mapi/identity/MapiIdentityMaterial.md)
- [MapiRequestIdentityScope](../../../../../classes/crates/lpe-exchange/src/mapi/identity/MapiRequestIdentityScope.md)
- [from_identity_records](../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiRequestIdentityScope/from_identity_records.md)
- [seed_from_identity_records](../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiRequestIdentityScope/seed_from_identity_records.md)
- [remember](../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiRequestIdentityScope/remember.md)
- [forget](../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiRequestIdentityScope/forget.md)
- [object_id](../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiRequestIdentityScope/object_id.md)
- [source_key](../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiRequestIdentityScope/source_key.md)
- [with_current_mapi_request_identity_scope](../../../../../functions/crates/lpe-exchange/src/mapi/identity/with_current_mapi_request_identity_scope.md)
- [MapiIdentityCodec](../../../../../classes/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec.md)
- [legacy_for_tests](../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/legacy_for_tests.md)
- [from_special_folder_identity_records](../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/from_special_folder_identity_records.md)
- [replica_guid](../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/replica_guid.md)
- [is_special_canonical_id](../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/is_special_canonical_id.md)
- [actual_object_id](../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/actual_object_id.md)
- [logical_object_id](../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/logical_object_id.md)
- [object_id_from_wire_id](../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/object_id_from_wire_id.md)
- [object_id_from_trailing_replid_wire_id](../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/object_id_from_trailing_replid_wire_id.md)
- [wire_id_bytes_from_object_id](../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/wire_id_bytes_from_object_id.md)
- [source_key_for_object_id](../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/source_key_for_object_id.md)
- [object_id_from_source_key](../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/object_id_from_source_key.md)
- [long_term_id_from_object_id](../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/long_term_id_from_object_id.md)
- [object_id_from_long_term_id](../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/object_id_from_long_term_id.md)
- [folder_entry_id_from_object_id](../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/folder_entry_id_from_object_id.md)
- [outlook_message_list_settings_entry_id](../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/outlook_message_list_settings_entry_id.md)
- [public_folder_entry_id_from_object_id](../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/public_folder_entry_id_from_object_id.md)
- [folder_entry_id_with_provider](../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/folder_entry_id_with_provider.md)
- [object_id_from_folder_entry_id](../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/object_id_from_folder_entry_id.md)
- [object_id_from_folder_identifier_bytes](../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/object_id_from_folder_identifier_bytes.md)
- [message_entry_id_from_object_ids](../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/message_entry_id_from_object_ids.md)
- [object_ids_from_message_entry_id](../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/object_ids_from_message_entry_id.md)
- [change_key_for_change_number](../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/change_key_for_change_number.md)
- [instance_key_for_object_id](../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/instance_key_for_object_id.md)
- [logical_special_folder_ids](../../../../../functions/crates/lpe-exchange/src/mapi/identity/logical_special_folder_ids.md)
- [is_logical_special_folder_id](../../../../../functions/crates/lpe-exchange/src/mapi/identity/is_logical_special_folder_id.md)
- [mapi_store_id](../../../../../functions/crates/lpe-exchange/src/mapi/identity/mapi_store_id.md)
- [mailbox_store_object_entry_id](../../../../../functions/crates/lpe-exchange/src/mapi/identity/mailbox_store_object_entry_id.md)
- [principal_mailbox_store_entry_id](../../../../../functions/crates/lpe-exchange/src/mapi/identity/principal_mailbox_store_entry_id.md)
- [global_counter_from_store_id](../../../../../functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_store_id.md)
- [globcnt_bytes](../../../../../functions/crates/lpe-exchange/src/mapi/identity/globcnt_bytes.md)
- [global_counter_from_globcnt](../../../../../functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_globcnt.md)
- [raw_object_id_from_wire_id](../../../../../functions/crates/lpe-exchange/src/mapi/identity/raw_object_id_from_wire_id.md)
- [raw_object_id_from_trailing_replid_wire_id](../../../../../functions/crates/lpe-exchange/src/mapi/identity/raw_object_id_from_trailing_replid_wire_id.md)
- [raw_wire_id_bytes_from_object_id](../../../../../functions/crates/lpe-exchange/src/mapi/identity/raw_wire_id_bytes_from_object_id.md)
- [object_id_from_wire_id](../../../../../functions/crates/lpe-exchange/src/mapi/identity/object_id_from_wire_id.md)
- [object_id_from_trailing_replid_wire_id](../../../../../functions/crates/lpe-exchange/src/mapi/identity/object_id_from_trailing_replid_wire_id.md)
- [wire_id_bytes_from_object_id](../../../../../functions/crates/lpe-exchange/src/mapi/identity/wire_id_bytes_from_object_id.md)
- [remember_mapi_identity](../../../../../functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity.md)
- [remember_mapi_identity_with_source_key](../../../../../functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity_with_source_key.md)
- [forget_mapi_identity](../../../../../functions/crates/lpe-exchange/src/mapi/identity/forget_mapi_identity.md)
- [mapped_mapi_object_id](../../../../../functions/crates/lpe-exchange/src/mapi/identity/mapped_mapi_object_id.md)
- [object_id_matches](../../../../../functions/crates/lpe-exchange/src/mapi/identity/object_id_matches.md)
- [mapped_mapi_source_key](../../../../../functions/crates/lpe-exchange/src/mapi/identity/mapped_mapi_source_key.md)
- [raw_long_term_id_from_object_id](../../../../../functions/crates/lpe-exchange/src/mapi/identity/raw_long_term_id_from_object_id.md)
- [raw_object_id_from_long_term_id](../../../../../functions/crates/lpe-exchange/src/mapi/identity/raw_object_id_from_long_term_id.md)
- [object_id_from_long_term_id_with_replica_guids](../../../../../functions/crates/lpe-exchange/src/mapi/identity/object_id_from_long_term_id_with_replica_guids.md)
- [raw_folder_entry_id_from_object_id](../../../../../functions/crates/lpe-exchange/src/mapi/identity/raw_folder_entry_id_from_object_id.md)
- [raw_outlook_message_list_settings_entry_id](../../../../../functions/crates/lpe-exchange/src/mapi/identity/raw_outlook_message_list_settings_entry_id.md)
- [raw_public_folder_entry_id_from_object_id](../../../../../functions/crates/lpe-exchange/src/mapi/identity/raw_public_folder_entry_id_from_object_id.md)
- [folder_entry_id_with_provider](../../../../../functions/crates/lpe-exchange/src/mapi/identity/folder_entry_id_with_provider.md)
- [folder_entry_id_with_provider_and_type](../../../../../functions/crates/lpe-exchange/src/mapi/identity/folder_entry_id_with_provider_and_type.md)
- [raw_object_id_from_folder_entry_id](../../../../../functions/crates/lpe-exchange/src/mapi/identity/raw_object_id_from_folder_entry_id.md)
- [raw_object_id_from_folder_identifier_bytes](../../../../../functions/crates/lpe-exchange/src/mapi/identity/raw_object_id_from_folder_identifier_bytes.md)
- [stale_special_folder_object_id_from_long_term_id](../../../../../functions/crates/lpe-exchange/src/mapi/identity/stale_special_folder_object_id_from_long_term_id.md)
- [is_advertised_special_folder_id](../../../../../functions/crates/lpe-exchange/src/mapi/identity/is_advertised_special_folder_id.md)
- [raw_message_entry_id_from_object_ids](../../../../../functions/crates/lpe-exchange/src/mapi/identity/raw_message_entry_id_from_object_ids.md)
- [raw_object_ids_from_message_entry_id](../../../../../functions/crates/lpe-exchange/src/mapi/identity/raw_object_ids_from_message_entry_id.md)
- [raw_source_key_for_object_id](../../../../../functions/crates/lpe-exchange/src/mapi/identity/raw_source_key_for_object_id.md)
- [generated_message_search_key](../../../../../functions/crates/lpe-exchange/src/mapi/identity/generated_message_search_key.md)
- [raw_object_id_from_source_key](../../../../../functions/crates/lpe-exchange/src/mapi/identity/raw_object_id_from_source_key.md)
- [raw_change_key_for_change_number](../../../../../functions/crates/lpe-exchange/src/mapi/identity/raw_change_key_for_change_number.md)
- [raw_instance_key_for_object_id](../../../../../functions/crates/lpe-exchange/src/mapi/identity/raw_instance_key_for_object_id.md)
- [long_term_id_from_object_id](../../../../../functions/crates/lpe-exchange/src/mapi/identity/long_term_id_from_object_id.md)
- [object_id_from_long_term_id](../../../../../functions/crates/lpe-exchange/src/mapi/identity/object_id_from_long_term_id.md)
- [folder_entry_id_from_object_id](../../../../../functions/crates/lpe-exchange/src/mapi/identity/folder_entry_id_from_object_id.md)
- [outlook_message_list_settings_entry_id](../../../../../functions/crates/lpe-exchange/src/mapi/identity/outlook_message_list_settings_entry_id.md)
- [public_folder_entry_id_from_object_id](../../../../../functions/crates/lpe-exchange/src/mapi/identity/public_folder_entry_id_from_object_id.md)
- [object_id_from_folder_entry_id](../../../../../functions/crates/lpe-exchange/src/mapi/identity/object_id_from_folder_entry_id.md)
- [object_id_from_folder_identifier_bytes](../../../../../functions/crates/lpe-exchange/src/mapi/identity/object_id_from_folder_identifier_bytes.md)
- [message_entry_id_from_object_ids](../../../../../functions/crates/lpe-exchange/src/mapi/identity/message_entry_id_from_object_ids.md)
- [object_ids_from_message_entry_id](../../../../../functions/crates/lpe-exchange/src/mapi/identity/object_ids_from_message_entry_id.md)
- [source_key_for_object_id](../../../../../functions/crates/lpe-exchange/src/mapi/identity/source_key_for_object_id.md)
- [object_id_from_source_key](../../../../../functions/crates/lpe-exchange/src/mapi/identity/object_id_from_source_key.md)
- [change_key_for_change_number](../../../../../functions/crates/lpe-exchange/src/mapi/identity/change_key_for_change_number.md)
- [instance_key_for_object_id](../../../../../functions/crates/lpe-exchange/src/mapi/identity/instance_key_for_object_id.md)
- [legacy_migration_object_id](../../../../../functions/crates/lpe-exchange/src/mapi/identity/legacy_migration_object_id.md)
- [scoped_codec_maps_logical_default_folder_ids_to_durable_ids](../../../../../functions/crates/lpe-exchange/src/mapi/identity/scoped_codec_maps_logical_default_folder_ids_to_durable_ids.md)
- [long_term_id_round_trips_object_id](../../../../../functions/crates/lpe-exchange/src/mapi/identity/long_term_id_round_trips_object_id.md)
- [folder_entry_id_round_trips_object_id](../../../../../functions/crates/lpe-exchange/src/mapi/identity/folder_entry_id_round_trips_object_id.md)
- [message_list_settings_entry_id_matches_exchange_private_shape](../../../../../functions/crates/lpe-exchange/src/mapi/identity/message_list_settings_entry_id_matches_exchange_private_shape.md)
- [public_folder_entry_id_uses_public_store_provider_uid](../../../../../functions/crates/lpe-exchange/src/mapi/identity/public_folder_entry_id_uses_public_store_provider_uid.md)
- [mailbox_store_object_entry_id_matches_outlook_wlink_shape](../../../../../functions/crates/lpe-exchange/src/mapi/identity/mailbox_store_object_entry_id_matches_outlook_wlink_shape.md)
- [message_entry_id_uses_private_mailbox_shape_with_source_key_counters](../../../../../functions/crates/lpe-exchange/src/mapi/identity/message_entry_id_uses_private_mailbox_shape_with_source_key_counters.md)
- [stale_cached_special_folder_identifiers_normalize_to_canonical_ids](../../../../../functions/crates/lpe-exchange/src/mapi/identity/stale_cached_special_folder_identifiers_normalize_to_canonical_ids.md)
- [stale_cached_conversation_history_identifier_is_not_advertised](../../../../../functions/crates/lpe-exchange/src/mapi/identity/stale_cached_conversation_history_identifier_is_not_advertised.md)
- [stale_cached_normal_item_identifiers_are_not_accepted_as_special_folders](../../../../../functions/crates/lpe-exchange/src/mapi/identity/stale_cached_normal_item_identifiers_are_not_accepted_as_special_folders.md)
- [wire_id_round_trips_replica_id_and_big_endian_global_counter](../../../../../functions/crates/lpe-exchange/src/mapi/identity/wire_id_round_trips_replica_id_and_big_endian_global_counter.md)
- [scoped_codec_accepts_legacy_logical_special_folder_wire_ids](../../../../../functions/crates/lpe-exchange/src/mapi/identity/scoped_codec_accepts_legacy_logical_special_folder_wire_ids.md)
- [source_change_and_instance_keys_are_replica_scoped](../../../../../functions/crates/lpe-exchange/src/mapi/identity/source_change_and_instance_keys_are_replica_scoped.md)
- [source_key_rejects_counters_outside_persisted_object_id_range](../../../../../functions/crates/lpe-exchange/src/mapi/identity/source_key_rejects_counters_outside_persisted_object_id_range.md)
- [dynamic_counters_start_after_reserved_special_folders](../../../../../functions/crates/lpe-exchange/src/mapi/identity/dynamic_counters_start_after_reserved_special_folders.md)
- [request_scope_keeps_special_folder_parent_identity_logical_and_durable](../../../../../functions/crates/lpe-exchange/src/mapi/identity/request_scope_keeps_special_folder_parent_identity_logical_and_durable.md)
- [owner_and_grantee_scopes_keep_hierarchy_folder_wire_ids_separate](../../../../../functions/crates/lpe-exchange/src/mapi/identity/owner_and_grantee_scopes_keep_hierarchy_folder_wire_ids_separate.md)
- [forgotten_mapi_identity_is_not_mapped](../../../../../functions/crates/lpe-exchange/src/mapi/identity/forgotten_mapi_identity_is_not_mapped.md)
- [source_key_rejects_non_mapi_object_id_instead_of_emitting_guid_only_xid](../../../../../functions/crates/lpe-exchange/src/mapi/identity/source_key_rejects_non_mapi_object_id_instead_of_emitting_guid_only_xid.md)

# Imports

- `super::*`
- `crate::store::{MapiIdentityObjectKind, MapiIdentityRecord, MapiIdentityRequest}`
- `anyhow::{anyhow, Result}`
- `std::sync::Arc`

# Member of

- [lpe-exchange](../../../../../packages/crates/lpe-exchange.md)