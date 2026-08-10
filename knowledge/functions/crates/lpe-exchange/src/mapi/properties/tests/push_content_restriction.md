---
type: Rust Function
title: push_content_restriction
resource: crates/lpe-exchange/src/mapi/properties/tests.rs#L2775-L2787
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/write_utf16z
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/tests/microsoft_oxcdata_reminder_restriction_example_parses_and_matches
---

# Signature

`fn push_content_restriction( restriction: &mut Vec<u8>, property_tag: u32, fuzzy_level_low: u16, value: &str, )`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [write_utf16z](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/write_utf16z.md)

# Called by

- [microsoft_oxcdata_reminder_restriction_example_parses_and_matches](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/microsoft_oxcdata_reminder_restriction_example_parses_and_matches.md)