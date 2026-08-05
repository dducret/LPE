---
type: Rust Function
title: precondition_not_modified
resource: crates/lpe-dav/src/preconditions.rs#L4-L6
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-dav/src/preconditions/match_condition_header
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-dav/src/service/DavService/handle_get
---

# Signature

`pub(crate) fn precondition_not_modified(headers: &HeaderMap, current_etag: &str) -> bool`

# Calls

- [match_condition_header](../../../../../functions/crates/lpe-dav/src/preconditions/match_condition_header.md)
- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [handle_get](../../../../../functions/crates/lpe-dav/src/service/DavService/handle_get.md)