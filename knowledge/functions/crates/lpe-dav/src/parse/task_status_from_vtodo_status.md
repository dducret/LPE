---
type: Rust Function
title: task_status_from_vtodo_status
resource: crates/lpe-dav/src/parse.rs#L310-L322
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-dav/src/parse/parse_vtodo
---

# Signature

`fn task_status_from_vtodo_status(status: &str) -> Result<String>`

# Called by

- [parse_vtodo](../../../../../functions/crates/lpe-dav/src/parse/parse_vtodo.md)