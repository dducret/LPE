---
type: TypeScript Function
title: sendStorageJson
resource: web/admin/src/StorageManagement.tsx#L410-L419
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/web/admin/src/StorageManagement/savePool
  - functions/web/admin/src/StorageManagement/savePolicy
---

# Signature

`async function sendStorageJson<T>(path: string, method: "POST" | "PUT", payload: unknown, token: string | null): Promise<T>`

# Called by

- [savePool](../../../../../functions/web/admin/src/StorageManagement/savePool.md)
- [savePolicy](../../../../../functions/web/admin/src/StorageManagement/savePolicy.md)