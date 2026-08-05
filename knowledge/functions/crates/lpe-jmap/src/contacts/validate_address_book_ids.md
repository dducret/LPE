---
type: Rust Function
title: validate_address_book_ids
resource: crates/lpe-jmap/src/contacts.rs#L876-L891
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/Parser/next
  - functions/crates/lpe-exchange/src/mapi/properties/values/MapiValue/as_bool
  called_by:
  - functions/crates/lpe-jmap/src/contacts/parse_contact_input
---

# Signature

`fn validate_address_book_ids(value: Option<&Value>) -> Result<Option<String>>`

# Calls

- [next](../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)
- [as_bool](../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/MapiValue/as_bool.md)

# Called by

- [parse_contact_input](../../../../../functions/crates/lpe-jmap/src/contacts/parse_contact_input.md)