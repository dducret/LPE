---
type: Rust Function
title: fake_contact_phone_by_label
resource: crates/lpe-exchange/src/tests/mod.rs#L4141-L4146
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/fake_contact_phone_values_by_label
  - functions/crates/lpe-core/src/sieve/Parser/next
  called_by:
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_address_book_entries
---

# Signature

`fn fake_contact_phone_by_label(contact: &AccessibleContact, labels: &[&str]) -> String`

# Calls

- [fake_contact_phone_values_by_label](../../../../../functions/crates/lpe-exchange/src/tests/fake_contact_phone_values_by_label.md)
- [next](../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)

# Called by

- [fetch_address_book_entries](../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_address_book_entries.md)