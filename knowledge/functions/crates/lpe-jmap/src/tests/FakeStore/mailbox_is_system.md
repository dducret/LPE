---
type: Rust Method
title: mailbox_is_system
resource: crates/lpe-jmap/src/tests.rs#L799-L807
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-jmap/src/tests/FakeStore/jmapstore/update_jmap_mailbox
  - functions/crates/lpe-jmap/src/tests/FakeStore/jmapstore/destroy_jmap_mailbox
---

# Signature

`fn mailbox_is_system(&self, mailbox_id: Uuid) -> bool`

# Called by

- [update_jmap_mailbox](../../../../../../functions/crates/lpe-jmap/src/tests/FakeStore/jmapstore/update_jmap_mailbox.md)
- [destroy_jmap_mailbox](../../../../../../functions/crates/lpe-jmap/src/tests/FakeStore/jmapstore/destroy_jmap_mailbox.md)