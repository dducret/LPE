---
type: Rust Function
title: jmap_problem
resource: crates/lpe-jmap/src/error.rs#L34-L51
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-jmap/src/error/http_error
  - functions/crates/lpe-jmap/src/error/jmap_problem_details_include_status_and_limit
  - functions/crates/lpe-jmap/src/service/api_handler
  - functions/crates/lpe-jmap/src/service/api_concurrency_limit
  - functions/crates/lpe-jmap/src/service/upload_concurrency_limit
---

# Signature

`pub(crate) fn jmap_problem( problem_type: &str, status: StatusCode, detail: impl Into<String>, limit: Option<&str>, ) -> (StatusCode, Json<Value>)`

# Called by

- [http_error](../../../../../functions/crates/lpe-jmap/src/error/http_error.md)
- [jmap_problem_details_include_status_and_limit](../../../../../functions/crates/lpe-jmap/src/error/jmap_problem_details_include_status_and_limit.md)
- [api_handler](../../../../../functions/crates/lpe-jmap/src/service/api_handler.md)
- [api_concurrency_limit](../../../../../functions/crates/lpe-jmap/src/service/api_concurrency_limit.md)
- [upload_concurrency_limit](../../../../../functions/crates/lpe-jmap/src/service/upload_concurrency_limit.md)