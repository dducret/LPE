---
type: Rust Function
title: finish_content_fai_debug_message
resource: crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec.rs#L377-L428
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/format_debug_hex
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/format_debug_hex_preview
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/format_debug_hex_tail
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_content_transfer_fai_debug_summary
---

# Signature

`fn finish_content_fai_debug_message( message: Option<ContentTransferMessageDebug>, final_cnset_seen_fai_counters: &[u64], fai_items: &mut Vec<ContentTransferFaiItemDebug>, bytes: &[u8], item_end_offset: usize, )`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [format_debug_hex](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/format_debug_hex.md)
- [format_debug_hex_preview](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/format_debug_hex_preview.md)
- [format_debug_hex_tail](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/format_debug_hex_tail.md)

# Called by

- [decode_content_transfer_fai_debug_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_content_transfer_fai_debug_summary.md)