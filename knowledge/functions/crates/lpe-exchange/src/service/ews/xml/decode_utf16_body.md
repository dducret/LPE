---
type: Rust Function
title: decode_utf16_body
resource: crates/lpe-exchange/src/service/ews/xml.rs#L62-L78
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/xml/decode_ews_body
---

# Signature

`fn decode_utf16_body(body: &[u8], little_endian: bool) -> Result<String>`

# Called by

- [decode_ews_body](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/decode_ews_body.md)