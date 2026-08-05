---
type: Rust Function
title: submit_message_rejects_non_accepted_success_body_before_smtp_final_reply
resource: LPE-CT/src/submission.rs#L950-L995
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/env_test_lock
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/build
---

# Signature

`async fn submit_message_rejects_non_accepted_success_body_before_smtp_final_reply()`

# Calls

- [env_test_lock](../../../../functions/LPE-CT/src/env_test_lock.md)
- [build](../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/build.md)