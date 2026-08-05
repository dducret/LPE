---
type: Rust Function
title: format_debug_hex_preview
resource: crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec.rs#L828-L830
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/format_debug_hex
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/finish_content_fai_debug_message
---

# Signature

`pub(super) fn format_debug_hex_preview(bytes: &[u8], max_len: usize) -> String`

# Calls

- [format_debug_hex](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/format_debug_hex.md)

# Called by

- [finish_content_fai_debug_message](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/finish_content_fai_debug_message.md)