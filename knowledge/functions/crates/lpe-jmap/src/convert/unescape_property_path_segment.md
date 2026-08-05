---
type: Rust Function
title: unescape_property_path_segment
resource: crates/lpe-jmap/src/convert.rs#L96-L112
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/Parser/next
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
---

# Signature

`fn unescape_property_path_segment(segment: &str) -> Result<String>`

# Calls

- [next](../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)
- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)