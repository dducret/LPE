---
type: Rust Function
title: convert_id_response_sources_for_element
resource: crates/lpe-exchange/src/tests/ews.rs#L4989-L5006
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/ews/test_attr
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/tests/ews/convert_id_response_sources
---

# Signature

`fn convert_id_response_sources_for_element(body: &str, element: &str) -> Vec<(String, String)>`

# Calls

- [test_attr](../../../../../../functions/crates/lpe-exchange/src/tests/ews/test_attr.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [convert_id_response_sources](../../../../../../functions/crates/lpe-exchange/src/tests/ews/convert_id_response_sources.md)