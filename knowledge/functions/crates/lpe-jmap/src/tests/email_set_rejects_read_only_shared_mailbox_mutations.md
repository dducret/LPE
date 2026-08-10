---
type: Rust Function
title: email_set_rejects_read_only_shared_mailbox_mutations
resource: crates/lpe-jmap/src/tests.rs#L3295-L3355
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/tests/FakeStore/shared_mailbox_read_only_access
  - functions/crates/lpe-jmap/src/service/JmapService/handle_api_request
---

# Signature

`async fn email_set_rejects_read_only_shared_mailbox_mutations()`

# Calls

- [shared_mailbox_read_only_access](../../../../../functions/crates/lpe-jmap/src/tests/FakeStore/shared_mailbox_read_only_access.md)
- [handle_api_request](../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_api_request.md)