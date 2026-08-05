---
type: Rust Function
title: collect
resource: crates/lpe-storage/tests/runtime_schema_drift.rs#L297-L305
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
---

# Signature

`fn collect<T>(failures: &mut Vec<String>, label: &str, result: Result<T>) -> Option<T>`

# Calls

- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)