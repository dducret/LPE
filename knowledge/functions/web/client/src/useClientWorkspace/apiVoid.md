---
type: TypeScript Function
title: apiVoid
resource: web/client/src/useClientWorkspace.ts#L66-L73
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/web/client/src/useClientWorkspace/notifySessionExpired
  called_by:
  - functions/web/client/src/useClientWorkspace/useClientWorkspace
---

# Signature

`async function apiVoid(path: string, options: RequestInit = {}): Promise<void>`

# Calls

- [notifySessionExpired](../../../../../functions/web/client/src/useClientWorkspace/notifySessionExpired.md)

# Called by

- [useClientWorkspace](../../../../../functions/web/client/src/useClientWorkspace/useClientWorkspace.md)