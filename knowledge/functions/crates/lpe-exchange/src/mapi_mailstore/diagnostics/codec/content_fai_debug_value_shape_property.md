---
type: Rust Function
title: content_fai_debug_value_shape_property
resource: crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec.rs#L430-L449
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/content_fai_debug_configuration_property
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_content_transfer_fai_debug_summary
---

# Signature

`pub(super) fn content_fai_debug_value_shape_property(tag: u32) -> bool`

# Calls

- [content_fai_debug_configuration_property](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/content_fai_debug_configuration_property.md)

# Called by

- [decode_content_transfer_fai_debug_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_content_transfer_fai_debug_summary.md)