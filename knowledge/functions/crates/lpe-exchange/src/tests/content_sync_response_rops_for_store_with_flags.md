---
type: Rust Function
title: content_sync_response_rops_for_store_with_flags
resource: crates/lpe-exchange/src/tests/mod.rs#L15535-L15579
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/mapi_headers
  - functions/crates/lpe-exchange/src/tests/mapi_cookie_header
  - functions/crates/lpe-exchange/src/tests/append_rop_open_folder
  - functions/crates/lpe-exchange/src/tests/append_rop_sync_manifest_get_buffer_with_state_and_flags
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
  - functions/crates/lpe-exchange/src/tests/execute_body
  - functions/crates/lpe-exchange/src/tests/response_rops_from_execute_response
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_common_views_non_wlink_fai_import_round_trips_durable_ics_identity_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_common_views_fai_table_open_and_ics_share_canonical_identity
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_outlook_mail_favorite_import_without_group_properties_persists
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/wlink_properties/mapi_over_http_wlink_client_properties_round_trip_postgresql_table_and_ics_after_reload
  - functions/crates/lpe-exchange/src/tests/content_sync_response_rops_for_store
---

# Signature

`async fn content_sync_response_rops_for_store_with_flags<S>( store: S, folder_id: u64, client_state: &[u8], synchronization_flags: u16, ) -> Vec<u8> where S: ExchangeStore + Clone + Send + Sync + 'static,`

# Calls

- [mapi_headers](../../../../../functions/crates/lpe-exchange/src/tests/mapi_headers.md)
- [mapi_cookie_header](../../../../../functions/crates/lpe-exchange/src/tests/mapi_cookie_header.md)
- [append_rop_open_folder](../../../../../functions/crates/lpe-exchange/src/tests/append_rop_open_folder.md)
- [append_rop_sync_manifest_get_buffer_with_state_and_flags](../../../../../functions/crates/lpe-exchange/src/tests/append_rop_sync_manifest_get_buffer_with_state_and_flags.md)
- [from_str](../../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)
- [execute_body](../../../../../functions/crates/lpe-exchange/src/tests/execute_body.md)
- [response_rops_from_execute_response](../../../../../functions/crates/lpe-exchange/src/tests/response_rops_from_execute_response.md)

# Called by

- [mapi_over_http_common_views_non_wlink_fai_import_round_trips_durable_ics_identity_in_postgresql](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_common_views_non_wlink_fai_import_round_trips_durable_ics_identity_in_postgresql.md)
- [mapi_over_http_common_views_fai_table_open_and_ics_share_canonical_identity](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_common_views_fai_table_open_and_ics_share_canonical_identity.md)
- [mapi_over_http_outlook_mail_favorite_import_without_group_properties_persists](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_outlook_mail_favorite_import_without_group_properties_persists.md)
- [mapi_over_http_wlink_client_properties_round_trip_postgresql_table_and_ics_after_reload](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/wlink_properties/mapi_over_http_wlink_client_properties_round_trip_postgresql_table_and_ics_after_reload.md)
- [content_sync_response_rops_for_store](../../../../../functions/crates/lpe-exchange/src/tests/content_sync_response_rops_for_store.md)