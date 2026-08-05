---
type: Rust Function
title: nspi_get_props_request
resource: crates/lpe-exchange/src/tests/mapi_over_http/nspi.rs#L16-L36
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
---

# Signature

`fn nspi_get_props_request(current_rec: u32, code_page: u32, tags: &[u32]) -> Vec<u8>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)