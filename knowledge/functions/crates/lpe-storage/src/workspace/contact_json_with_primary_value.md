---
type: Rust Function
title: contact_json_with_primary_value
resource: crates/lpe-storage/src/workspace.rs#L1302-L1324
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-storage/src/workspace/merge_contact_update_input
---

# Signature

`fn contact_json_with_primary_value(existing: &Value, key: &str, label: &str, value: &str) -> Value`

# Calls

- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [merge_contact_update_input](../../../../../functions/crates/lpe-storage/src/workspace/merge_contact_update_input.md)