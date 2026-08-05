---
type: Rust Function
title: decode_content_transfer_fai_debug_summary
resource: crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec.rs#L198-L375
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/read_debug_u32
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/content_debug_marker
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/finish_content_fai_debug_message
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/parse_debug_fast_transfer_property
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/format_replguid_globset_debug
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/replguid_globset_counters
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/content_fai_debug_value_shape_property
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/fast_transfer_value_shape
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/counter_from_xid
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_debug_bool
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_debug_i32
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_debug_object_id
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_debug_change_number
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_debug_utf16z
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_debug_string8z
  called_by:
  - functions/crates/lpe-exchange/src/mapi/sync/tests/common_views_fai_fasttransfer_boundaries_cover_only_persisted_shortcuts
  - functions/crates/lpe-exchange/src/mapi/sync/tests/inbox_fai_fasttransfer_boundaries_export_only_persisted_fai
  - functions/crates/lpe-exchange/src/mapi/sync/tests/empty_persisted_inbox_named_view_is_exported_by_fai_sync
  - functions/crates/lpe-exchange/src/mapi/sync/tests/outlook_inbox_fai_ics_omits_unsupported_message_identity_properties
  - functions/crates/lpe-exchange/src/mapi/sync/tests/associated_config_fai_content_sync_emits_valid_property_definitions
  - functions/crates/lpe-exchange/src/mapi/sync/tests/inbox_associated_content_sync_payload_emits_required_fai_properties
  - functions/crates/lpe-exchange/src/mapi/sync/tests/common_views_associated_content_sync_payload_emits_view_and_wunderbar_properties
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/log_fai_content_sync_debug
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/content_sync_manifest_starts_fai_message_before_item_properties
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/content_sync_manifest_unicode_fai_uses_unicode_subject_and_fai_message_flag
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/special_message_headers_and_final_cnsets_share_durable_change_numbers
---

# Signature

`pub(crate) fn decode_content_transfer_fai_debug_summary( bytes: &[u8], ) -> Result<ContentTransferFaiDebugSummary, String>`

# Calls

- [read_debug_u32](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/read_debug_u32.md)
- [content_debug_marker](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/content_debug_marker.md)
- [finish_content_fai_debug_message](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/finish_content_fai_debug_message.md)
- [parse_debug_fast_transfer_property](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/parse_debug_fast_transfer_property.md)
- [format_replguid_globset_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/format_replguid_globset_debug.md)
- [replguid_globset_counters](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/replguid_globset_counters.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [content_fai_debug_value_shape_property](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/content_fai_debug_value_shape_property.md)
- [fast_transfer_value_shape](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/fast_transfer_value_shape.md)
- [counter_from_xid](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/counter_from_xid.md)
- [decode_debug_bool](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_debug_bool.md)
- [decode_debug_i32](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_debug_i32.md)
- [decode_debug_object_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_debug_object_id.md)
- [decode_debug_change_number](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_debug_change_number.md)
- [decode_debug_utf16z](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_debug_utf16z.md)
- [decode_debug_string8z](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_debug_string8z.md)

# Called by

- [common_views_fai_fasttransfer_boundaries_cover_only_persisted_shortcuts](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/common_views_fai_fasttransfer_boundaries_cover_only_persisted_shortcuts.md)
- [inbox_fai_fasttransfer_boundaries_export_only_persisted_fai](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/inbox_fai_fasttransfer_boundaries_export_only_persisted_fai.md)
- [empty_persisted_inbox_named_view_is_exported_by_fai_sync](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/empty_persisted_inbox_named_view_is_exported_by_fai_sync.md)
- [outlook_inbox_fai_ics_omits_unsupported_message_identity_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/outlook_inbox_fai_ics_omits_unsupported_message_identity_properties.md)
- [associated_config_fai_content_sync_emits_valid_property_definitions](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/associated_config_fai_content_sync_emits_valid_property_definitions.md)
- [inbox_associated_content_sync_payload_emits_required_fai_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/inbox_associated_content_sync_payload_emits_required_fai_properties.md)
- [common_views_associated_content_sync_payload_emits_view_and_wunderbar_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/common_views_associated_content_sync_payload_emits_view_and_wunderbar_properties.md)
- [log_fai_content_sync_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/log_fai_content_sync_debug.md)
- [content_sync_manifest_starts_fai_message_before_item_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/content_sync_manifest_starts_fai_message_before_item_properties.md)
- [content_sync_manifest_unicode_fai_uses_unicode_subject_and_fai_message_flag](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/content_sync_manifest_unicode_fai_uses_unicode_subject_and_fai_message_flag.md)
- [special_message_headers_and_final_cnsets_share_durable_change_numbers](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/special_message_headers_and_final_cnsets_share_durable_change_numbers.md)