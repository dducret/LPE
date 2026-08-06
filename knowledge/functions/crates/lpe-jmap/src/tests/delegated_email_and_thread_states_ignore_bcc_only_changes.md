---
type: Rust Function
title: delegated_email_and_thread_states_ignore_bcc_only_changes
resource: crates/lpe-jmap/src/tests.rs#L3930-L3977
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/tests/FakeStore/draft_email
  - functions/crates/lpe-jmap/src/service/object_state/JmapService/mail_object_state
---

# Signature

`async fn delegated_email_and_thread_states_ignore_bcc_only_changes()`

# Calls

- [draft_email](../../../../../functions/crates/lpe-jmap/src/tests/FakeStore/draft_email.md)
- [mail_object_state](../../../../../functions/crates/lpe-jmap/src/service/object_state/JmapService/mail_object_state.md)