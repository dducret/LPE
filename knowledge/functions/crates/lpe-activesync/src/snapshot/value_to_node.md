---
type: Rust Function
title: value_to_node
resource: crates/lpe-activesync/src/snapshot.rs#L558-L581
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-activesync/src/service/sync_helpers/value_to_wbxml
---

# Signature

`pub(crate) fn value_to_node(data: &serde_json::Map<String, Value>) -> WbxmlNode`

# Calls

- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [value_to_wbxml](../../../../../functions/crates/lpe-activesync/src/service/sync_helpers/value_to_wbxml.md)