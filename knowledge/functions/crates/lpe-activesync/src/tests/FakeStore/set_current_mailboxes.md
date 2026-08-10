---
type: Rust Method
title: set_current_mailboxes
resource: crates/lpe-activesync/src/tests.rs#L259-L261
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-activesync/src/tests/FakeStore/activesyncstore/create_jmap_mailbox
  - functions/crates/lpe-activesync/src/tests/FakeStore/activesyncstore/update_jmap_mailbox
  - functions/crates/lpe-activesync/src/tests/FakeStore/activesyncstore/destroy_jmap_mailbox
---

# Signature

`fn set_current_mailboxes(&self, mailboxes: Vec<JmapMailbox>)`

# Called by

- [create_jmap_mailbox](../../../../../../functions/crates/lpe-activesync/src/tests/FakeStore/activesyncstore/create_jmap_mailbox.md)
- [update_jmap_mailbox](../../../../../../functions/crates/lpe-activesync/src/tests/FakeStore/activesyncstore/update_jmap_mailbox.md)
- [destroy_jmap_mailbox](../../../../../../functions/crates/lpe-activesync/src/tests/FakeStore/activesyncstore/destroy_jmap_mailbox.md)