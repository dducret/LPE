---
type: Rust Function
title: get_free_busy
resource: crates/lpe-admin-api/src/delegation.rs#L277-L314
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/access/require_account
  - functions/crates/lpe-admin-api/src/http/bad_request_error
  - functions/crates/lpe-storage/src/collaboration/Storage/fetch_delegate_access_objects
  - functions/crates/lpe-storage/src/collaboration/Storage/fetch_free_busy_blocks
  - functions/crates/lpe-storage/src/collaboration/Storage/project_delegate_freebusy_messages
---

# Signature

`pub(crate) async fn get_free_busy( State(storage): State<Storage>, headers: HeaderMap, Query(query): Query<FreeBusyQuery>, ) -> ApiResult<FreeBusyResponse>`

# Calls

- [require_account](../../../../../functions/crates/lpe-admin-api/src/access/require_account.md)
- [bad_request_error](../../../../../functions/crates/lpe-admin-api/src/http/bad_request_error.md)
- [fetch_delegate_access_objects](../../../../../functions/crates/lpe-storage/src/collaboration/Storage/fetch_delegate_access_objects.md)
- [fetch_free_busy_blocks](../../../../../functions/crates/lpe-storage/src/collaboration/Storage/fetch_free_busy_blocks.md)
- [project_delegate_freebusy_messages](../../../../../functions/crates/lpe-storage/src/collaboration/Storage/project_delegate_freebusy_messages.md)