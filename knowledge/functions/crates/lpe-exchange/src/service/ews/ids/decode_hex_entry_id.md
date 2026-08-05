---
type: Rust Function
title: decode_hex_entry_id
resource: crates/lpe-exchange/src/service/ews/ids.rs#L244-L255
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/ids/canonical_ews_object_id_from_convert_source
---

# Signature

`fn decode_hex_entry_id(value: &str) -> Result<String>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [canonical_ews_object_id_from_convert_source](../../../../../../../functions/crates/lpe-exchange/src/service/ews/ids/canonical_ews_object_id_from_convert_source.md)