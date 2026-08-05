---
type: Rust Function
title: project_share
resource: crates/lpe-jmap/src/store/shares.rs#L5-L86
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-jmap/src/store/shares/copy_share_field
  - functions/crates/lpe-jmap/src/store/shares/copy_share_field_as
  - functions/crates/lpe-jmap/src/store/shares/share_rights
  called_by:
  - functions/crates/lpe-jmap/src/store/Storage/jmapstore/fetch_jmap_shares
  - functions/crates/lpe-jmap/src/store/Storage/jmapstore/upsert_jmap_share
---

# Signature

`pub(super) fn project_share(share_type: &str, value: Value) -> Result<Value>`

# Calls

- [get](../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [copy_share_field](../../../../../../functions/crates/lpe-jmap/src/store/shares/copy_share_field.md)
- [copy_share_field_as](../../../../../../functions/crates/lpe-jmap/src/store/shares/copy_share_field_as.md)
- [share_rights](../../../../../../functions/crates/lpe-jmap/src/store/shares/share_rights.md)

# Called by

- [fetch_jmap_shares](../../../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/fetch_jmap_shares.md)
- [upsert_jmap_share](../../../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/upsert_jmap_share.md)