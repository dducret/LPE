---
type: Rust Function
title: create_trace_dir
resource: crates/lpe-core/src/outlook_trace.rs#L108-L111
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-core/src/outlook_trace/write_event
---

# Signature

`fn create_trace_dir(path: &Path) -> std::io::Result<()>`

# Called by

- [write_event](../../../../../functions/crates/lpe-core/src/outlook_trace/write_event.md)