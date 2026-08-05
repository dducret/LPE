---
type: Rust Method
title: email_addresses
resource: crates/lpe-exchange/src/tests/mod.rs#L4490-L4498
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/import_jmap_email
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/save_draft_message
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/submit_message
---

# Signature

`fn email_addresses(recipients: &[SubmittedRecipientInput]) -> Vec<JmapEmailAddress>`

# Called by

- [import_jmap_email](../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/import_jmap_email.md)
- [save_draft_message](../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/save_draft_message.md)
- [submit_message](../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/submit_message.md)