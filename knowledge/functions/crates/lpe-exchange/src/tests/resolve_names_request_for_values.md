---
type: Rust Function
title: resolve_names_request_for_values
resource: crates/lpe-exchange/src/tests/mod.rs#L12658-L12680
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_resolve_names_projects_each_requested_recipient
  - functions/crates/lpe-exchange/src/tests/resolve_names_request
---

# Signature

`fn resolve_names_request_for_values(search_addresses: &[&str], columns: &[u32]) -> Vec<u8>`

# Calls

- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [mapi_over_http_resolve_names_projects_each_requested_recipient](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_resolve_names_projects_each_requested_recipient.md)
- [resolve_names_request](../../../../../functions/crates/lpe-exchange/src/tests/resolve_names_request.md)