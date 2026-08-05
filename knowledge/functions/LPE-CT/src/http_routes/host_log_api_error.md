---
type: Rust Function
title: host_log_api_error
resource: LPE-CT/src/http_routes.rs#L419-L421
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/host_logs/HostLogError/status
  - functions/LPE-CT/src/host_logs/HostLogError/message
---

# Signature

`fn host_log_api_error(error: host_logs::HostLogError) -> ApiError`

# Calls

- [status](../../../../functions/LPE-CT/src/host_logs/HostLogError/status.md)
- [message](../../../../functions/LPE-CT/src/host_logs/HostLogError/message.md)