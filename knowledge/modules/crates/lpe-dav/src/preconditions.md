---
type: Rust Module
title: preconditions
resource: crates/lpe-dav/src/preconditions.rs#L1-L58
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-bail-result
  - external/axum-http-headermap-headervalue
  member_of:
  - packages/crates/lpe-dav
---

# Contains

- [precondition_not_modified](../../../../functions/crates/lpe-dav/src/preconditions/precondition_not_modified.md)
- [check_write_preconditions](../../../../functions/crates/lpe-dav/src/preconditions/check_write_preconditions.md)
- [check_delete_preconditions](../../../../functions/crates/lpe-dav/src/preconditions/check_delete_preconditions.md)
- [match_condition_header](../../../../functions/crates/lpe-dav/src/preconditions/match_condition_header.md)

# Imports

- `anyhow::{bail, Result}`
- `axum::http::{HeaderMap, HeaderValue}`

# Member of

- [lpe-dav](../../../../packages/crates/lpe-dav.md)