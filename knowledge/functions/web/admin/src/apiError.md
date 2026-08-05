---
type: TypeScript Function
title: apiError
resource: web/admin/src/main.tsx#L88
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/web/admin/src/fetchJson
  - functions/web/admin/src/sendJson
  - functions/web/admin/src/sendDelete
  - functions/web/admin/src/sendFormData
---

# Signature

`async function apiError(response: Response, path: string): Promise<Error>`

# Called by

- [fetchJson](../../../../functions/web/admin/src/fetchJson.md)
- [sendJson](../../../../functions/web/admin/src/sendJson.md)
- [sendDelete](../../../../functions/web/admin/src/sendDelete.md)
- [sendFormData](../../../../functions/web/admin/src/sendFormData.md)