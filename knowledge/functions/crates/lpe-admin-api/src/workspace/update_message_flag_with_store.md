---
type: Rust Function
title: update_message_flag_with_store
resource: crates/lpe-admin-api/src/workspace.rs#L414-L435
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/workspace/require_account_from_store
  - functions/crates/lpe-admin-api/src/workspace/map_update_message_flag_request
  called_by:
  - functions/crates/lpe-admin-api/src/workspace/update_message_flag
  - functions/crates/lpe-admin-api/src/workspace/tests/update_message_flag_handler_uses_canonical_flag_store_path
---

# Signature

`async fn update_message_flag_with_store<S: ClientSubmissionStore>( storage: &S, headers: &HeaderMap, message_id: Uuid, request: UpdateMessageFlagRequest, ) -> std::result::Result<(), (StatusCode, String)>`

# Calls

- [require_account_from_store](../../../../../functions/crates/lpe-admin-api/src/workspace/require_account_from_store.md)
- [map_update_message_flag_request](../../../../../functions/crates/lpe-admin-api/src/workspace/map_update_message_flag_request.md)

# Called by

- [update_message_flag](../../../../../functions/crates/lpe-admin-api/src/workspace/update_message_flag.md)
- [update_message_flag_handler_uses_canonical_flag_store_path](../../../../../functions/crates/lpe-admin-api/src/workspace/tests/update_message_flag_handler_uses_canonical_flag_store_path.md)