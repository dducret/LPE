---
type: Rust Function
title: legacy_dn_matches_entry
resource: crates/lpe-exchange/src/mapi/rop/recipients.rs#L302-L305
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/recipients/legacy_dn_recipient_address
---

# Signature

`fn legacy_dn_matches_entry(actual: &str, expected: &str) -> bool`

# Called by

- [legacy_dn_recipient_address](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/recipients/legacy_dn_recipient_address.md)