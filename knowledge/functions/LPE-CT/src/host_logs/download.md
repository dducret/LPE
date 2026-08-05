---
type: Rust Function
title: download
resource: LPE-CT/src/host_logs.rs#L131-L145
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/host_logs/resolve_log
  - functions/LPE-CT/src/host_logs/io_error
  called_by:
  - functions/LPE-CT/src/http_routes/download_host_log
---

# Signature

`pub(crate) fn download(category: &str, id: &str) -> Result<HostLogDownload, HostLogError>`

# Calls

- [resolve_log](../../../../functions/LPE-CT/src/host_logs/resolve_log.md)
- [io_error](../../../../functions/LPE-CT/src/host_logs/io_error.md)

# Called by

- [download_host_log](../../../../functions/LPE-CT/src/http_routes/download_host_log.md)