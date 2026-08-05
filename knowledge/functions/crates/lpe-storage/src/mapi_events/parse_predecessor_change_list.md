---
type: Rust Function
title: parse_predecessor_change_list
resource: crates/lpe-storage/src/mapi_events.rs#L1437-L1466
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
---

# Signature

`fn parse_predecessor_change_list(bytes: &[u8]) -> Result<BTreeMap<[u8; 16], Vec<u8>>>`

# Calls

- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)