---
type: Rust Module
title: error
resource: crates/lpe-jmap/src/error.rs#L1-L92
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-error
  - external/axum-http-statuscode-json
  - external/serde-json-json-value
  - external/super
  member_of:
  - packages/crates/lpe-jmap
---

# Contains

- [http_error](../../../../functions/crates/lpe-jmap/src/error/http_error.md)
- [jmap_problem](../../../../functions/crates/lpe-jmap/src/error/jmap_problem.md)
- [method_error](../../../../functions/crates/lpe-jmap/src/error/method_error.md)
- [method_error_from_error](../../../../functions/crates/lpe-jmap/src/error/method_error_from_error.md)
- [set_error](../../../../functions/crates/lpe-jmap/src/error/set_error.md)
- [jmap_problem_details_include_status_and_limit](../../../../functions/crates/lpe-jmap/src/error/jmap_problem_details_include_status_and_limit.md)

# Imports

- `anyhow::Error`
- `axum::{http::StatusCode, Json}`
- `serde_json::{json, Value}`
- `super::*`

# Member of

- [lpe-jmap](../../../../packages/crates/lpe-jmap.md)