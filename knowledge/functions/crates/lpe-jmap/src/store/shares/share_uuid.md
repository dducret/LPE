---
type: Rust Function
title: share_uuid
resource: crates/lpe-jmap/src/store/shares.rs#L130-L137
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-jmap/src/store/Storage/jmapstore/delete_jmap_share
---

# Signature

`pub(super) fn share_uuid(share: &Value, field: &str) -> Result<Uuid>`

# Calls

- [get](../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [delete_jmap_share](../../../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/delete_jmap_share.md)