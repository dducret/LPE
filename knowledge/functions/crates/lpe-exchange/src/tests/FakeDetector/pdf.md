---
type: Rust Method
title: pdf
resource: crates/lpe-exchange/src/tests/mod.rs#L4209-L4220
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/tests/ews/create_attachment_validates_and_adds_canonical_attachment
  - functions/crates/lpe-exchange/src/tests/ews/create_attachment_rejects_unknown_parent_message
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_attachment_waits_for_parent_save_and_is_handle_local
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_create_commits_event_and_attachment_together
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_custom_collection_attachment_is_hidden_for_existing_guarded_event
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_create_attachment_saves_canonical_attachment_from_properties
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_write_stream_saves_canonical_attachment
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_copy_to_stream_saves_canonical_attachment
  - functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_emsmdb_logon_uses_authenticated_canonical_principal
  - functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_emsmdb_query_rows_reads_root_hierarchy_without_ipm_children
  - functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_emsmdb_rpc_ext2_parse_failure_returns_protocol_fault
  - functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_emsmdb_rpc_ext2_requires_authenticated_context
  - functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_address_book_check_name_fallback_answers_framing_mismatch
  - functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_address_book_auth3_does_not_trigger_check_name_fallback
  - functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_address_book_management_stats_accepts_rca_short_stub
  - functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_referral_get_fqdn_accepts_rca_short_stub
---

# Signature

`fn pdf() -> Self`

# Called by

- [create_attachment_validates_and_adds_canonical_attachment](../../../../../../functions/crates/lpe-exchange/src/tests/ews/create_attachment_validates_and_adds_canonical_attachment.md)
- [create_attachment_rejects_unknown_parent_message](../../../../../../functions/crates/lpe-exchange/src/tests/ews/create_attachment_rejects_unknown_parent_message.md)
- [mapi_over_http_calendar_attachment_waits_for_parent_save_and_is_handle_local](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_attachment_waits_for_parent_save_and_is_handle_local.md)
- [mapi_over_http_calendar_create_commits_event_and_attachment_together](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_create_commits_event_and_attachment_together.md)
- [mapi_over_http_calendar_custom_collection_attachment_is_hidden_for_existing_guarded_event](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_custom_collection_attachment_is_hidden_for_existing_guarded_event.md)
- [mapi_over_http_create_attachment_saves_canonical_attachment_from_properties](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_create_attachment_saves_canonical_attachment_from_properties.md)
- [mapi_over_http_write_stream_saves_canonical_attachment](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_write_stream_saves_canonical_attachment.md)
- [mapi_over_http_copy_to_stream_saves_canonical_attachment](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_copy_to_stream_saves_canonical_attachment.md)
- [rpc_proxy_emsmdb_logon_uses_authenticated_canonical_principal](../../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_emsmdb_logon_uses_authenticated_canonical_principal.md)
- [rpc_proxy_emsmdb_query_rows_reads_root_hierarchy_without_ipm_children](../../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_emsmdb_query_rows_reads_root_hierarchy_without_ipm_children.md)
- [rpc_proxy_emsmdb_rpc_ext2_parse_failure_returns_protocol_fault](../../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_emsmdb_rpc_ext2_parse_failure_returns_protocol_fault.md)
- [rpc_proxy_emsmdb_rpc_ext2_requires_authenticated_context](../../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_emsmdb_rpc_ext2_requires_authenticated_context.md)
- [rpc_proxy_address_book_check_name_fallback_answers_framing_mismatch](../../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_address_book_check_name_fallback_answers_framing_mismatch.md)
- [rpc_proxy_address_book_auth3_does_not_trigger_check_name_fallback](../../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_address_book_auth3_does_not_trigger_check_name_fallback.md)
- [rpc_proxy_address_book_management_stats_accepts_rca_short_stub](../../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_address_book_management_stats_accepts_rca_short_stub.md)
- [rpc_proxy_referral_get_fqdn_accepts_rca_short_stub](../../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_referral_get_fqdn_accepts_rca_short_stub.md)