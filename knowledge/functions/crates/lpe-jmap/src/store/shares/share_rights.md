---
type: Rust Function
title: share_rights
resource: crates/lpe-jmap/src/store/shares.rs#L103-L112
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-jmap/src/store/shares/project_share
---

# Signature

`fn share_rights(object: &Map<String, Value>) -> Option<Value>`

# Calls

- [get](../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [project_share](../../../../../../functions/crates/lpe-jmap/src/store/shares/project_share.md)