---
type: Rust Function
title: delete
resource: LPE-CT/src/host_logs.rs#L147-L158
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/host_logs/resolve_log
  - functions/LPE-CT/src/host_logs/io_error
  called_by:
  - functions/LPE-CT/src/http_routes/delete_host_log
  - functions/LPE-CT/src/router
  - functions/LPE-CT/web/app/handleBodyChange
  - functions/LPE-CT/web/app/smoke/test/MockClassList/remove
  - functions/LPE-CT/web/app/smoke/test/MockClassList/toggle
  - functions/LPE-CT/web/app/smoke/test/createContext
  - functions/LPE-CT/web/modules/app/lists/pruneQuarantineSelection
  - functions/crates/lpe-admin-api/src/app/router
---

# Signature

`pub(crate) fn delete(category: &str, id: &str) -> Result<String, HostLogError>`

# Calls

- [resolve_log](../../../../functions/LPE-CT/src/host_logs/resolve_log.md)
- [io_error](../../../../functions/LPE-CT/src/host_logs/io_error.md)

# Called by

- [delete_host_log](../../../../functions/LPE-CT/src/http_routes/delete_host_log.md)
- [router](../../../../functions/LPE-CT/src/router.md)
- [handleBodyChange](../../../../functions/LPE-CT/web/app/handleBodyChange.md)
- [remove](../../../../functions/LPE-CT/web/app/smoke/test/MockClassList/remove.md)
- [toggle](../../../../functions/LPE-CT/web/app/smoke/test/MockClassList/toggle.md)
- [createContext](../../../../functions/LPE-CT/web/app/smoke/test/createContext.md)
- [pruneQuarantineSelection](../../../../functions/LPE-CT/web/modules/app/lists/pruneQuarantineSelection.md)
- [router](../../../../functions/crates/lpe-admin-api/src/app/router.md)