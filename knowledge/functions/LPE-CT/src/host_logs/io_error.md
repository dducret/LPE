---
type: Rust Function
title: io_error
resource: LPE-CT/src/host_logs.rs#L283-L285
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/host_logs/read_content
  - functions/LPE-CT/src/host_logs/download
  - functions/LPE-CT/src/host_logs/delete
  - functions/LPE-CT/src/host_logs/discover_log_names
---

# Signature

`fn io_error(status: StatusCode, action: &str, error: io::Error) -> HostLogError`

# Called by

- [read_content](../../../../functions/LPE-CT/src/host_logs/read_content.md)
- [download](../../../../functions/LPE-CT/src/host_logs/download.md)
- [delete](../../../../functions/LPE-CT/src/host_logs/delete.md)
- [discover_log_names](../../../../functions/LPE-CT/src/host_logs/discover_log_names.md)