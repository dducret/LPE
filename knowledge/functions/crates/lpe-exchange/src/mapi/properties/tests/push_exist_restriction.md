---
type: Rust Function
title: push_exist_restriction
resource: crates/lpe-exchange/src/mapi/properties/tests.rs#L2682-L2685
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/tests/microsoft_oxcdata_reminder_restriction_example_parses_and_matches
---

# Signature

`fn push_exist_restriction(restriction: &mut Vec<u8>, property_tag: u32)`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [microsoft_oxcdata_reminder_restriction_example_parses_and_matches](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/microsoft_oxcdata_reminder_restriction_example_parses_and_matches.md)