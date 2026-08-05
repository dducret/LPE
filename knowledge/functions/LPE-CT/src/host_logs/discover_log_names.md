---
type: Rust Function
title: discover_log_names
resource: LPE-CT/src/host_logs.rs#L187-L219
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/host_logs/io_error
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/LPE-CT/src/host_logs/list
---

# Signature

`fn discover_log_names(log_dir: &Path, current_name: &str) -> Result<Vec<String>, HostLogError>`

# Calls

- [io_error](../../../../functions/LPE-CT/src/host_logs/io_error.md)
- [push](../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [list](../../../../functions/LPE-CT/src/host_logs/list.md)