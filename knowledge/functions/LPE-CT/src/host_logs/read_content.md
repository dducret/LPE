---
type: Rust Function
title: read_content
resource: LPE-CT/src/host_logs.rs#L96-L129
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/host_logs/resolve_log
  - functions/LPE-CT/src/host_logs/is_previewable
  - functions/LPE-CT/src/host_logs/io_error
  called_by:
  - functions/LPE-CT/src/http_routes/host_log_content
---

# Signature

`pub(crate) fn read_content(category: &str, id: &str) -> Result<HostLogContent, HostLogError>`

# Calls

- [resolve_log](../../../../functions/LPE-CT/src/host_logs/resolve_log.md)
- [is_previewable](../../../../functions/LPE-CT/src/host_logs/is_previewable.md)
- [io_error](../../../../functions/LPE-CT/src/host_logs/io_error.md)

# Called by

- [host_log_content](../../../../functions/LPE-CT/src/http_routes/host_log_content.md)