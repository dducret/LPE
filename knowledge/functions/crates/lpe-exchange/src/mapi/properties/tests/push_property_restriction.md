---
type: Rust Function
title: push_property_restriction
resource: crates/lpe-exchange/src/mapi/properties/tests.rs#L2762-L2773
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/tests/microsoft_oxcdata_reminder_restriction_example_parses_and_matches
---

# Signature

`fn push_property_restriction( restriction: &mut Vec<u8>, relop: u8, property_tag: u32, value: &MapiValue, )`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [write_mapi_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value.md)

# Called by

- [microsoft_oxcdata_reminder_restriction_example_parses_and_matches](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/microsoft_oxcdata_reminder_restriction_example_parses_and_matches.md)