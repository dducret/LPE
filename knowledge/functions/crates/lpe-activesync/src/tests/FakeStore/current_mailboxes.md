---
type: Rust Method
title: current_mailboxes
resource: crates/lpe-activesync/src/tests.rs#L250-L256
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-activesync/src/tests/FakeStore/activesyncstore/fetch_jmap_mailboxes
  - functions/crates/lpe-activesync/src/tests/FakeStore/activesyncstore/create_jmap_mailbox
  - functions/crates/lpe-activesync/src/tests/FakeStore/activesyncstore/update_jmap_mailbox
  - functions/crates/lpe-activesync/src/tests/FakeStore/activesyncstore/destroy_jmap_mailbox
---

# Signature

`fn current_mailboxes(&self) -> Vec<JmapMailbox>`

# Called by

- [fetch_jmap_mailboxes](../../../../../../functions/crates/lpe-activesync/src/tests/FakeStore/activesyncstore/fetch_jmap_mailboxes.md)
- [create_jmap_mailbox](../../../../../../functions/crates/lpe-activesync/src/tests/FakeStore/activesyncstore/create_jmap_mailbox.md)
- [update_jmap_mailbox](../../../../../../functions/crates/lpe-activesync/src/tests/FakeStore/activesyncstore/update_jmap_mailbox.md)
- [destroy_jmap_mailbox](../../../../../../functions/crates/lpe-activesync/src/tests/FakeStore/activesyncstore/destroy_jmap_mailbox.md)