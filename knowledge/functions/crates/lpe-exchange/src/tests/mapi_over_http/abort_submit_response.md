---
type: Rust Function
title: abort_submit_response
resource: crates/lpe-exchange/src/tests/mapi_over_http.rs#L704-L749
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/legacy_migration_object_id
  - functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity
  - functions/crates/lpe-exchange/src/tests/mapi_headers
  - functions/crates/lpe-exchange/src/tests/mapi_cookie_header
  - functions/crates/lpe-exchange/src/tests/append_mapi_wire_id
  - functions/crates/lpe-exchange/src/tests/test_mapi_folder_id
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
  - functions/crates/lpe-exchange/src/tests/execute_body
  - functions/crates/lpe-exchange/src/tests/response_rops_from_execute_response
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_microsoft_abort_submit_cancels_pre_handoff_submission
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_microsoft_abort_submit_rejects_handed_off_and_terminal_submissions
---

# Signature

`async fn abort_submit_response(status: &str) -> (Vec<u8>, Vec<Uuid>)`

# Calls

- [legacy_migration_object_id](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/legacy_migration_object_id.md)
- [remember_mapi_identity](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity.md)
- [mapi_headers](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_headers.md)
- [mapi_cookie_header](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_cookie_header.md)
- [append_mapi_wire_id](../../../../../../functions/crates/lpe-exchange/src/tests/append_mapi_wire_id.md)
- [test_mapi_folder_id](../../../../../../functions/crates/lpe-exchange/src/tests/test_mapi_folder_id.md)
- [from_str](../../../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)
- [execute_body](../../../../../../functions/crates/lpe-exchange/src/tests/execute_body.md)
- [response_rops_from_execute_response](../../../../../../functions/crates/lpe-exchange/src/tests/response_rops_from_execute_response.md)

# Called by

- [mapi_over_http_microsoft_abort_submit_cancels_pre_handoff_submission](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_microsoft_abort_submit_cancels_pre_handoff_submission.md)
- [mapi_over_http_microsoft_abort_submit_rejects_handed_off_and_terminal_submissions](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_microsoft_abort_submit_rejects_handed_off_and_terminal_submissions.md)