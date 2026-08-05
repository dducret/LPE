---
type: Rust Function
title: open_trace_file
resource: crates/lpe-core/src/outlook_trace.rs#L124-L128
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-core/src/outlook_trace/write_event
---

# Signature

`fn open_trace_file(path: &Path) -> std::io::Result<std::fs::File>`

# Called by

- [write_event](../../../../../functions/crates/lpe-core/src/outlook_trace/write_event.md)