---
type: Rust Function
title: format_replguid_globset_debug
resource: crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec.rs#L1103-L1136
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/format_debug_hex
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_globset_ranges
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/hierarchy_semantic_validation
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_content_transfer_fai_debug_summary
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/collect_final_state_debug_property
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/replguid_globset_debug_summary
---

# Signature

`pub(super) fn format_replguid_globset_debug(value: &[u8]) -> String`

# Calls

- [format_debug_hex](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/format_debug_hex.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [decode_globset_ranges](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_globset_ranges.md)

# Called by

- [hierarchy_semantic_validation](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/hierarchy_semantic_validation.md)
- [decode_content_transfer_fai_debug_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_content_transfer_fai_debug_summary.md)
- [collect_final_state_debug_property](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/collect_final_state_debug_property.md)
- [replguid_globset_debug_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/replguid_globset_debug_summary.md)