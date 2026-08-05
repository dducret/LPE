---
type: Rust Function
title: copy_share_field_as
resource: crates/lpe-jmap/src/store/shares.rs#L92-L101
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-jmap/src/store/shares/project_share
  - functions/crates/lpe-jmap/src/store/shares/copy_share_field
---

# Signature

`fn copy_share_field_as( source: &Map<String, Value>, target: &mut Map<String, Value>, source_field: &str, target_field: &str, )`

# Calls

- [get](../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [project_share](../../../../../../functions/crates/lpe-jmap/src/store/shares/project_share.md)
- [copy_share_field](../../../../../../functions/crates/lpe-jmap/src/store/shares/copy_share_field.md)