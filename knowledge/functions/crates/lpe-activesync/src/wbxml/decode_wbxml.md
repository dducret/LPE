---
type: Rust Function
title: decode_wbxml
resource: crates/lpe-activesync/src/wbxml.rs#L107-L125
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/read_multibyte_int
  - functions/crates/lpe-activesync/src/wbxml/parse_node
  called_by:
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/handle_parsed_request
  - functions/crates/lpe-activesync/src/service/submission/ActiveSyncService/handle_send_mail
  - functions/crates/lpe-activesync/src/tests/decode_response_body
  - functions/crates/lpe-activesync/src/tests/wbxml_roundtrip_preserves_tokens_and_text
  - functions/crates/lpe-activesync/src/tests/wbxml_decode_preserves_unsupported_enum_boundaries
  - functions/crates/lpe-activesync/src/tests/wbxml_roundtrip_preserves_get_item_estimate_tokens
---

# Signature

`pub(crate) fn decode_wbxml(bytes: &[u8]) -> Result<WbxmlNode>`

# Calls

- [read_multibyte_int](../../../../../functions/crates/lpe-activesync/src/wbxml/read_multibyte_int.md)
- [parse_node](../../../../../functions/crates/lpe-activesync/src/wbxml/parse_node.md)

# Called by

- [handle_parsed_request](../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/handle_parsed_request.md)
- [handle_send_mail](../../../../../functions/crates/lpe-activesync/src/service/submission/ActiveSyncService/handle_send_mail.md)
- [decode_response_body](../../../../../functions/crates/lpe-activesync/src/tests/decode_response_body.md)
- [wbxml_roundtrip_preserves_tokens_and_text](../../../../../functions/crates/lpe-activesync/src/tests/wbxml_roundtrip_preserves_tokens_and_text.md)
- [wbxml_decode_preserves_unsupported_enum_boundaries](../../../../../functions/crates/lpe-activesync/src/tests/wbxml_decode_preserves_unsupported_enum_boundaries.md)
- [wbxml_roundtrip_preserves_get_item_estimate_tokens](../../../../../functions/crates/lpe-activesync/src/tests/wbxml_roundtrip_preserves_get_item_estimate_tokens.md)