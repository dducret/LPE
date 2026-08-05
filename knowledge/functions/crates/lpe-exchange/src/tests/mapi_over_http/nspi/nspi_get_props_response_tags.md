---
type: Rust Function
title: nspi_get_props_response_tags
resource: crates/lpe-exchange/src/tests/mapi_over_http/nspi.rs#L79-L149
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_null_get_props_matches_entry_prop_list
---

# Signature

`fn nspi_get_props_response_tags(body: &[u8]) -> Vec<u32>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [mapi_over_http_nspi_null_get_props_matches_entry_prop_list](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_null_get_props_matches_entry_prop_list.md)