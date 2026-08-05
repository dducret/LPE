---
type: Rust Function
title: trace_file_paths
resource: crates/lpe-core/src/outlook_trace.rs#L152-L164
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-core/src/outlook_trace/write_event
---

# Signature

`fn trace_file_paths(directory: &Path, component: &str, session_key: &str) -> TraceFilePaths`

# Called by

- [write_event](../../../../../functions/crates/lpe-core/src/outlook_trace/write_event.md)