---
type: Rust Function
title: copy_share_field
resource: crates/lpe-jmap/src/store/shares.rs#L88-L90
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/store/shares/copy_share_field_as
  called_by:
  - functions/crates/lpe-jmap/src/store/shares/project_share
---

# Signature

`fn copy_share_field(source: &Map<String, Value>, target: &mut Map<String, Value>, field: &str)`

# Calls

- [copy_share_field_as](../../../../../../functions/crates/lpe-jmap/src/store/shares/copy_share_field_as.md)

# Called by

- [project_share](../../../../../../functions/crates/lpe-jmap/src/store/shares/project_share.md)