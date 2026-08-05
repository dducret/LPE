---
type: Rust Function
title: update_message_flag
resource: crates/lpe-admin-api/src/workspace.rs#L389-L400
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/workspace/update_message_flag_with_store
---

# Signature

`pub(crate) async fn update_message_flag( State(storage): State<Storage>, headers: HeaderMap, AxumPath(message_id): AxumPath<Uuid>, Json(request): Json<UpdateMessageFlagRequest>, ) -> ApiResult<HealthResponse>`

# Calls

- [update_message_flag_with_store](../../../../../functions/crates/lpe-admin-api/src/workspace/update_message_flag_with_store.md)