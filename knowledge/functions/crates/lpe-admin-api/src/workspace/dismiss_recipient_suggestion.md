---
type: Rust Function
title: dismiss_recipient_suggestion
resource: crates/lpe-admin-api/src/workspace.rs#L770-L784
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/access/require_account
---

# Signature

`pub(crate) async fn dismiss_recipient_suggestion( State(storage): State<Storage>, headers: HeaderMap, AxumPath(suggestion_id): AxumPath<Uuid>, ) -> ApiResult<HealthResponse>`

# Calls

- [require_account](../../../../../functions/crates/lpe-admin-api/src/access/require_account.md)