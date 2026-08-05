---
type: Rust Function
title: fake_contact_phone_values_by_label
resource: crates/lpe-exchange/src/tests/mod.rs#L4020-L4040
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/tests/fake_contact_phone_by_label
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_address_book_entries
---

# Signature

`fn fake_contact_phone_values_by_label(contact: &AccessibleContact, labels: &[&str]) -> Vec<String>`

# Calls

- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [fake_contact_phone_by_label](../../../../../functions/crates/lpe-exchange/src/tests/fake_contact_phone_by_label.md)
- [fetch_address_book_entries](../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_address_book_entries.md)