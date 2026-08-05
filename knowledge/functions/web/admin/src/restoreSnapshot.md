---
type: TypeScript Function
title: restoreSnapshot
resource: web/admin/src/main.tsx#L257-L263
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/load
  - functions/crates/lpe-exchange/src/mapi_mailstore/FastTransferDirectPropertyFilter/includes
  called_by:
  - functions/web/admin/src/App
---

# Signature

`async function restoreSnapshot(snapshot: SnapshotRecord)`

# Calls

- [load](../../../../functions/LPE-CT/web/app/load.md)
- [includes](../../../../functions/crates/lpe-exchange/src/mapi_mailstore/FastTransferDirectPropertyFilter/includes.md)

# Called by

- [App](../../../../functions/web/admin/src/App.md)