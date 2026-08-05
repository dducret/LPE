---
type: Rust Function
title: nspi_get_props_without_property_tags_request
resource: crates/lpe-exchange/src/tests/mapi_over_http/nspi.rs#L38-L58
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

`fn nspi_get_props_without_property_tags_request( flags: u32, current_rec: u32, code_page: u32, ) -> Vec<u8>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [mapi_over_http_nspi_null_get_props_matches_entry_prop_list](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_null_get_props_matches_entry_prop_list.md)