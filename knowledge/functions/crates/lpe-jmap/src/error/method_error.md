---
type: Rust Function
title: method_error
resource: crates/lpe-jmap/src/error.rs#L53-L58
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-jmap/src/blob/JmapService/handle_blob_lookup
  - functions/crates/lpe-jmap/src/error/method_error_from_error
  - functions/crates/lpe-jmap/src/error/set_error
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_email_submission_set
  - functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account
  - functions/crates/lpe-jmap/src/service/helpers/result_reference_error
  - functions/crates/lpe-jmap/src/service/helpers/method_object_limit_error
---

# Signature

`pub(crate) fn method_error(kind: &str, description: &str) -> Value`

# Called by

- [handle_blob_lookup](../../../../../functions/crates/lpe-jmap/src/blob/JmapService/handle_blob_lookup.md)
- [method_error_from_error](../../../../../functions/crates/lpe-jmap/src/error/method_error_from_error.md)
- [set_error](../../../../../functions/crates/lpe-jmap/src/error/set_error.md)
- [handle_email_submission_set](../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_email_submission_set.md)
- [handle_api_request_for_account](../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account.md)
- [result_reference_error](../../../../../functions/crates/lpe-jmap/src/service/helpers/result_reference_error.md)
- [method_object_limit_error](../../../../../functions/crates/lpe-jmap/src/service/helpers/method_object_limit_error.md)