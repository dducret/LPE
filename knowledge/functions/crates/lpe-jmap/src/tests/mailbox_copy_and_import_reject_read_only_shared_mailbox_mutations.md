---
type: Rust Function
title: mailbox_copy_and_import_reject_read_only_shared_mailbox_mutations
resource: crates/lpe-jmap/src/tests.rs#L7284-L7373
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/tests/FakeStore/shared_mailbox_read_only_access
  - functions/crates/lpe-jmap/src/tests/FakeStore/shared_account
  - functions/crates/lpe-jmap/src/tests/FakeStore/draft_email
  - functions/crates/lpe-jmap/src/service/JmapService/handle_api_request
---

# Signature

`async fn mailbox_copy_and_import_reject_read_only_shared_mailbox_mutations()`

# Calls

- [shared_mailbox_read_only_access](../../../../../functions/crates/lpe-jmap/src/tests/FakeStore/shared_mailbox_read_only_access.md)
- [shared_account](../../../../../functions/crates/lpe-jmap/src/tests/FakeStore/shared_account.md)
- [draft_email](../../../../../functions/crates/lpe-jmap/src/tests/FakeStore/draft_email.md)
- [handle_api_request](../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_api_request.md)