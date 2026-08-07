---
type: Rust Function
title: preserve_empty
resource: crates/lpe-admin-api/src/workspace.rs#L890-L896
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-admin-api/src/workspace/upsert_client_event
---

# Signature

`fn preserve_empty(value: String, existing: Option<String>) -> String`

# Called by

- [upsert_client_event](../../../../../functions/crates/lpe-admin-api/src/workspace/upsert_client_event.md)