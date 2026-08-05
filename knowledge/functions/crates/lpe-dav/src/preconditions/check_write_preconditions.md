---
type: Rust Function
title: check_write_preconditions
resource: crates/lpe-dav/src/preconditions.rs#L8-L28
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-dav/src/preconditions/match_condition_header
  called_by:
  - functions/crates/lpe-dav/src/service/DavService/handle_put
---

# Signature

`pub(crate) fn check_write_preconditions( headers: &HeaderMap, current_etag: Option<String>, ) -> Result<()>`

# Calls

- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [match_condition_header](../../../../../functions/crates/lpe-dav/src/preconditions/match_condition_header.md)

# Called by

- [handle_put](../../../../../functions/crates/lpe-dav/src/service/DavService/handle_put.md)