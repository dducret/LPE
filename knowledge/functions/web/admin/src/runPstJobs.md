---
type: TypeScript Function
title: runPstJobs
resource: web/admin/src/main.tsx#L212-L217
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/web/admin/src/sendJson
  - functions/LPE-CT/web/app/load
  - functions/crates/lpe-exchange/src/mapi_mailstore/FastTransferDirectPropertyFilter/includes
  called_by:
  - functions/web/admin/src/App
---

# Signature

`async function runPstJobs()`

# Calls

- [sendJson](../../../../functions/web/admin/src/sendJson.md)
- [load](../../../../functions/LPE-CT/web/app/load.md)
- [includes](../../../../functions/crates/lpe-exchange/src/mapi_mailstore/FastTransferDirectPropertyFilter/includes.md)

# Called by

- [App](../../../../functions/web/admin/src/App.md)