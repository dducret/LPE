---
type: Rust Function
title: next_trace_sequence
resource: crates/lpe-core/src/outlook_trace.rs#L166-L172
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/test_rca_outlook_trace_summary/FakePath/open
  called_by:
  - functions/crates/lpe-core/src/outlook_trace/write_event
---

# Signature

`fn next_trace_sequence(path: &Path) -> std::io::Result<u64>`

# Calls

- [open](../../../../../functions/tools/test_rca_outlook_trace_summary/FakePath/open.md)

# Called by

- [write_event](../../../../../functions/crates/lpe-core/src/outlook_trace/write_event.md)