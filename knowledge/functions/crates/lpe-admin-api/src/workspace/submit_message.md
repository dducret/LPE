---
type: Rust Function
title: submit_message
resource: crates/lpe-admin-api/src/workspace.rs#L308-L316
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/workspace/submit_message_with_store
---

# Signature

`pub(crate) async fn submit_message( State(storage): State<Storage>, headers: HeaderMap, Json(request): Json<SubmitMessageRequest>, ) -> ApiResult<SubmittedMessage>`

# Calls

- [submit_message_with_store](../../../../../functions/crates/lpe-admin-api/src/workspace/submit_message_with_store.md)