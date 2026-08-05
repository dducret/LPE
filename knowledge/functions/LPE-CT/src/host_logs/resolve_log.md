---
type: Rust Function
title: resolve_log
resource: LPE-CT/src/host_logs.rs#L253-L270
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/host_logs/category_definition
  - functions/LPE-CT/src/host_logs/is_allowed_log_name
  - functions/LPE-CT/src/host_logs/host_log_dir
  called_by:
  - functions/LPE-CT/src/host_logs/read_content
  - functions/LPE-CT/src/host_logs/download
  - functions/LPE-CT/src/host_logs/delete
---

# Signature

`fn resolve_log(category: &str, id: &str) -> Result<ResolvedLog, HostLogError>`

# Calls

- [category_definition](../../../../functions/LPE-CT/src/host_logs/category_definition.md)
- [is_allowed_log_name](../../../../functions/LPE-CT/src/host_logs/is_allowed_log_name.md)
- [host_log_dir](../../../../functions/LPE-CT/src/host_logs/host_log_dir.md)

# Called by

- [read_content](../../../../functions/LPE-CT/src/host_logs/read_content.md)
- [download](../../../../functions/LPE-CT/src/host_logs/download.md)
- [delete](../../../../functions/LPE-CT/src/host_logs/delete.md)