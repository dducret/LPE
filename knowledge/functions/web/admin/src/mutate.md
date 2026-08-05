---
type: TypeScript Function
title: mutate
resource: web/admin/src/main.tsx#L198-L203
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/FastTransferDirectPropertyFilter/includes
  called_by:
  - functions/web/admin/src/App
---

# Signature

`async function mutate(action: string, path: string, method: "POST" | "PUT", payload: unknown, success: string, afterSuccess?: () => void)`

# Calls

- [includes](../../../../functions/crates/lpe-exchange/src/mapi_mailstore/FastTransferDirectPropertyFilter/includes.md)

# Called by

- [App](../../../../functions/web/admin/src/App.md)