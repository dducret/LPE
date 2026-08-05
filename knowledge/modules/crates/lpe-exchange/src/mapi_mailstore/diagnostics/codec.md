---
type: Rust Module
title: codec
resource: crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec.rs#L1-L1294
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/super
  member_of:
  - packages/crates/lpe-exchange
---

# Contains

- [FastTransferDebugProperty](../../../../../../classes/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/FastTransferDebugProperty.md)
- [ContentTransferFaiDebugSummary](../../../../../../classes/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/ContentTransferFaiDebugSummary.md)
- [ContentTransferFaiItemDebug](../../../../../../classes/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/ContentTransferFaiItemDebug.md)
- [ContentTransferMessageDebug](../../../../../../classes/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/ContentTransferMessageDebug.md)
- [decode_hierarchy_transfer_debug_summary](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_hierarchy_transfer_debug_summary.md)
- [decode_content_transfer_fai_debug_summary](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_content_transfer_fai_debug_summary.md)
- [finish_content_fai_debug_message](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/finish_content_fai_debug_message.md)
- [content_fai_debug_value_shape_property](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/content_fai_debug_value_shape_property.md)
- [content_fai_debug_configuration_property](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/content_fai_debug_configuration_property.md)
- [fast_transfer_value_shape](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/fast_transfer_value_shape.md)
- [collect_final_state_debug_property](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/collect_final_state_debug_property.md)
- [finalize_hierarchy_debug_summary](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/finalize_hierarchy_debug_summary.md)
- [hierarchy_row_server_change_number](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/hierarchy_row_server_change_number.md)
- [counters_include_all](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/counters_include_all.md)
- [finish_hierarchy_debug_folder](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/finish_hierarchy_debug_folder.md)
- [missing_hierarchy_core_property_tags](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/missing_hierarchy_core_property_tags.md)
- [property_position](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/property_position.md)
- [hierarchy_identity_properties_before_display_name](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/hierarchy_identity_properties_before_display_name.md)
- [decode_debug_i32](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_debug_i32.md)
- [decode_debug_i16](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_debug_i16.md)
- [decode_debug_u64](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_debug_u64.md)
- [decode_debug_object_id](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_debug_object_id.md)
- [decode_debug_change_number](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_debug_change_number.md)
- [decode_debug_bool](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_debug_bool.md)
- [decode_debug_utf16z](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_debug_utf16z.md)
- [decode_debug_string8z](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_debug_string8z.md)
- [format_debug_hex](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/format_debug_hex.md)
- [format_debug_hex_preview](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/format_debug_hex_preview.md)
- [format_debug_hex_tail](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/format_debug_hex_tail.md)
- [format_u64_hex](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/format_u64_hex.md)
- [format_property_tag_names](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/format_property_tag_names.md)
- [property_tag_debug_name](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/property_tag_debug_name.md)
- [hierarchy_debug_known_parent_source_key](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/hierarchy_debug_known_parent_source_key.md)
- [hierarchy_debug_marker](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/hierarchy_debug_marker.md)
- [content_debug_marker](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/content_debug_marker.md)
- [fast_transfer_marker_debug_name](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/fast_transfer_marker_debug_name.md)
- [parse_debug_fast_transfer_property](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/parse_debug_fast_transfer_property.md)
- [read_debug_u32](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/read_debug_u32.md)
- [read_debug_slice](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/read_debug_slice.md)
- [format_usize_list](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/format_usize_list.md)
- [format_property_tags](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/format_property_tags.md)
- [format_property_value_shapes](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/format_property_value_shapes.md)
- [replguid_globset_debug_summary](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/replguid_globset_debug_summary.md)
- [final_sync_state_debug_summary](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/final_sync_state_debug_summary.md)
- [format_marker_tags](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/format_marker_tags.md)
- [format_replguid_globset_debug](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/format_replguid_globset_debug.md)
- [replguid_globset_counters](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/replguid_globset_counters.md)
- [decode_globset_ranges](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_globset_ranges.md)
- [globcnt_slice_to_u64](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/globcnt_slice_to_u64.md)
- [coalesced_u8_ranges](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/coalesced_u8_ranges.md)
- [counter_from_xid](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/counter_from_xid.md)

# Imports

- `super::*`

# Member of

- [lpe-exchange](../../../../../../packages/crates/lpe-exchange.md)