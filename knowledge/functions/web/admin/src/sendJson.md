---
type: TypeScript Function
title: sendJson
resource: web/admin/src/main.tsx#L90
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/web/admin/src/apiError
  called_by:
  - functions/web/admin/src/runPstJobs
---

# Signature

`async function sendJson<T>(path: string, method: "POST" | "PUT", payload: unknown, token: string | null): Promise<T>`

# Calls

- [apiError](../../../../functions/web/admin/src/apiError.md)

# Called by

- [runPstJobs](../../../../functions/web/admin/src/runPstJobs.md)