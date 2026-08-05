---
type: Rust Function
title: check_delete_preconditions
resource: crates/lpe-dav/src/preconditions.rs#L30-L48
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-dav/src/preconditions/match_condition_header
  called_by:
  - functions/crates/lpe-dav/src/service/DavService/handle_delete
---

# Signature

`pub(crate) fn check_delete_preconditions( headers: &HeaderMap, current_etag: Option<String>, ) -> Result<()>`

# Calls

- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [match_condition_header](../../../../../functions/crates/lpe-dav/src/preconditions/match_condition_header.md)

# Called by

- [handle_delete](../../../../../functions/crates/lpe-dav/src/service/DavService/handle_delete.md)