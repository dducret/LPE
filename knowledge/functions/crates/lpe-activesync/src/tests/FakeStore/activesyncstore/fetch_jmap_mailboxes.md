---
type: Rust Method
title: fetch_jmap_mailboxes
resource: crates/lpe-activesync/src/tests.rs#L318-L325
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-activesync/src/tests/FakeStore/current_mailboxes
---

# Signature

`fn fetch_jmap_mailboxes<'a>(&'a self, account_id: Uuid) -> StoreFuture<'a, Vec<JmapMailbox>>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [current_mailboxes](../../../../../../../functions/crates/lpe-activesync/src/tests/FakeStore/current_mailboxes.md)