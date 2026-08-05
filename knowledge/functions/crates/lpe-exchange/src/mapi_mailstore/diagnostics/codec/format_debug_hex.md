---
type: Rust Function
title: format_debug_hex
resource: crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec.rs#L824-L826
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-domain/src/crypto/hex_lower
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/hierarchy_semantic_validation
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/finish_content_fai_debug_message
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/finish_hierarchy_debug_folder
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/format_debug_hex_preview
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/format_debug_hex_tail
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/format_replguid_globset_debug
---

# Signature

`pub(super) fn format_debug_hex(bytes: &[u8]) -> String`

# Calls

- [hex_lower](../../../../../../../functions/crates/lpe-domain/src/crypto/hex_lower.md)

# Called by

- [hierarchy_semantic_validation](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/hierarchy_semantic_validation.md)
- [finish_content_fai_debug_message](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/finish_content_fai_debug_message.md)
- [finish_hierarchy_debug_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/finish_hierarchy_debug_folder.md)
- [format_debug_hex_preview](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/format_debug_hex_preview.md)
- [format_debug_hex_tail](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/format_debug_hex_tail.md)
- [format_replguid_globset_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/format_replguid_globset_debug.md)