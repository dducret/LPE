---
type: Rust Function
title: http_error
resource: crates/lpe-jmap/src/error.rs#L10-L32
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/error/jmap_problem
---

# Signature

`pub(crate) fn http_error(error: Error) -> (StatusCode, Json<Value>)`

# Calls

- [jmap_problem](../../../../../functions/crates/lpe-jmap/src/error/jmap_problem.md)