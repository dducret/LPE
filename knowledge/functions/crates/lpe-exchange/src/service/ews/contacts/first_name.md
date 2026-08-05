---
type: Rust Function
title: first_name
resource: crates/lpe-exchange/src/service/ews/contacts.rs#L615-L620
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/Parser/next
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/contacts/contact_given_name
---

# Signature

`fn first_name(name: &str) -> String`

# Calls

- [next](../../../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)

# Called by

- [contact_given_name](../../../../../../../functions/crates/lpe-exchange/src/service/ews/contacts/contact_given_name.md)