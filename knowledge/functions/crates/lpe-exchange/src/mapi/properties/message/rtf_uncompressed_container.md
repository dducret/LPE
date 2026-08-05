---
type: Rust Function
title: rtf_uncompressed_container
resource: crates/lpe-exchange/src/mapi/properties/message.rs#L343-L355
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from
  - functions/crates/lpe-core/src/sieve/Parser/expect
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/message/uncompressed_rtf_body
---

# Signature

`fn rtf_uncompressed_container(raw: &[u8]) -> Vec<u8>`

# Calls

- [try_from](../../../../../../../functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from.md)
- [expect](../../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)

# Called by

- [uncompressed_rtf_body](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/uncompressed_rtf_body.md)