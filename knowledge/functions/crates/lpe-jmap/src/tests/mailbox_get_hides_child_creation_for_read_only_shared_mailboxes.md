---
type: Rust Function
title: mailbox_get_hides_child_creation_for_read_only_shared_mailboxes
resource: crates/lpe-jmap/src/tests.rs#L6884-L6939
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/tests/FakeStore/shared_mailbox_read_only_access
  - functions/crates/lpe-jmap/src/tests/FakeStore/shared_account
  - functions/crates/lpe-jmap/src/service/JmapService/handle_api_request
---

# Signature

`async fn mailbox_get_hides_child_creation_for_read_only_shared_mailboxes()`

# Calls

- [shared_mailbox_read_only_access](../../../../../functions/crates/lpe-jmap/src/tests/FakeStore/shared_mailbox_read_only_access.md)
- [shared_account](../../../../../functions/crates/lpe-jmap/src/tests/FakeStore/shared_account.md)
- [handle_api_request](../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_api_request.md)