---
type: Rust Function
title: classify_client_mailbox_access_error
resource: crates/lpe-admin-api/src/workspace/mailbox_access.rs#L25-L34
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/http/internal_error
---

# Signature

`pub(crate) fn classify_client_mailbox_access_error(error: anyhow::Error) -> (StatusCode, String)`

# Calls

- [internal_error](../../../../../../functions/crates/lpe-admin-api/src/http/internal_error.md)