---
type: Rust Function
title: with_current_mapi_identity_codec
resource: crates/lpe-exchange/src/mapi/identity.rs#L36-L41
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute_response
  - functions/crates/lpe-exchange/src/mapi/identity/scoped_codec_maps_logical_default_folder_ids_to_durable_ids
  - functions/crates/lpe-exchange/src/mapi/identity/request_scope_keeps_special_folder_parent_identity_logical_and_durable
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/execute_rpc_emsmdb_rops
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/scoped_final_sync_state_uses_the_durable_inbox_counter
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_selective_reopen_uses_durable_event_modseq
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_move_copy_messages_uses_canonical_store
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_fast_transfer_get_buffer_resumes_across_execute_requests
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_replays_outlook_contact_sync_import_then_save
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/hierarchy/mapi_over_http_microsoft_oxcfold_create_delete_and_move_use_canonical_mailboxes
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/hierarchy/mapi_over_http_move_folder_rejects_wrong_source_parent_without_side_effects
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/hierarchy/mapi_over_http_microsoft_folder_move_accepts_nonzero_boolean_fields_and_copy_rejects
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_content_sync_incremental_after_client_state_exports_delta
---

# Signature

`pub(crate) async fn with_current_mapi_identity_codec<T>( codec: MapiIdentityCodec, future: impl std::future::Future<Output = T>, ) -> T`

# Called by

- [execute_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_response.md)
- [scoped_codec_maps_logical_default_folder_ids_to_durable_ids](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/scoped_codec_maps_logical_default_folder_ids_to_durable_ids.md)
- [request_scope_keeps_special_folder_parent_identity_logical_and_durable](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/request_scope_keeps_special_folder_parent_identity_logical_and_durable.md)
- [execute_rpc_emsmdb_rops](../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/execute_rpc_emsmdb_rops.md)
- [scoped_final_sync_state_uses_the_durable_inbox_counter](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/scoped_final_sync_state_uses_the_durable_inbox_counter.md)
- [mapi_over_http_calendar_selective_reopen_uses_durable_event_modseq](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_selective_reopen_uses_durable_event_modseq.md)
- [mapi_over_http_move_copy_messages_uses_canonical_store](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_move_copy_messages_uses_canonical_store.md)
- [mapi_over_http_fast_transfer_get_buffer_resumes_across_execute_requests](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_fast_transfer_get_buffer_resumes_across_execute_requests.md)
- [mapi_over_http_replays_outlook_contact_sync_import_then_save](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_replays_outlook_contact_sync_import_then_save.md)
- [mapi_over_http_microsoft_oxcfold_create_delete_and_move_use_canonical_mailboxes](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/hierarchy/mapi_over_http_microsoft_oxcfold_create_delete_and_move_use_canonical_mailboxes.md)
- [mapi_over_http_move_folder_rejects_wrong_source_parent_without_side_effects](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/hierarchy/mapi_over_http_move_folder_rejects_wrong_source_parent_without_side_effects.md)
- [mapi_over_http_microsoft_folder_move_accepts_nonzero_boolean_fields_and_copy_rejects](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/hierarchy/mapi_over_http_microsoft_folder_move_accepts_nonzero_boolean_fields_and_copy_rejects.md)
- [mapi_over_http_content_sync_incremental_after_client_state_exports_delta](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_content_sync_incremental_after_client_state_exports_delta.md)