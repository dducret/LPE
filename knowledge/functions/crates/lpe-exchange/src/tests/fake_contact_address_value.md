---
type: Rust Function
title: fake_contact_address_value
resource: crates/lpe-exchange/src/tests/mod.rs#L4099-L4113
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_address_book_entries
---

# Signature

`fn fake_contact_address_value(contact: &AccessibleContact, keys: &[&str]) -> String`

# Calls

- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [fetch_address_book_entries](../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_address_book_entries.md)