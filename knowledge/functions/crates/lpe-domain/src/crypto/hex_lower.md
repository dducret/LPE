---
type: Rust Function
title: hex_lower
resource: crates/lpe-domain/src/crypto.rs#L6-L15
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-domain/src/crypto/sha256_hex
  - functions/crates/lpe-domain/src/crypto/hmac_sha256_hex
  - functions/crates/lpe-exchange/src/mapi/rop/debug/shapes/hex_preview_for_debug
  - functions/crates/lpe-exchange/src/mapi/tables/contents/category_value_to_string
  - functions/crates/lpe-exchange/src/mapi/tables/diagnostics/format_debug_binary
  - functions/crates/lpe-exchange/src/mapi/transport/headers/hex_preview
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/format_debug_hex
  - functions/crates/lpe-storage/src/storage_backend/percent_encode_segment
---

# Signature

`pub fn hex_lower(bytes: impl AsRef<[u8]>) -> String`

# Calls

- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [sha256_hex](../../../../../functions/crates/lpe-domain/src/crypto/sha256_hex.md)
- [hmac_sha256_hex](../../../../../functions/crates/lpe-domain/src/crypto/hmac_sha256_hex.md)
- [hex_preview_for_debug](../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/shapes/hex_preview_for_debug.md)
- [category_value_to_string](../../../../../functions/crates/lpe-exchange/src/mapi/tables/contents/category_value_to_string.md)
- [format_debug_binary](../../../../../functions/crates/lpe-exchange/src/mapi/tables/diagnostics/format_debug_binary.md)
- [hex_preview](../../../../../functions/crates/lpe-exchange/src/mapi/transport/headers/hex_preview.md)
- [format_debug_hex](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/format_debug_hex.md)
- [percent_encode_segment](../../../../../functions/crates/lpe-storage/src/storage_backend/percent_encode_segment.md)