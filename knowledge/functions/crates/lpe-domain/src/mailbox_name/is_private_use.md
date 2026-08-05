---
type: Rust Function
title: is_private_use
resource: crates/lpe-domain/src/mailbox_name.rs#L386-L391
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-domain/src/mailbox_name/validate_codepoints
---

# Signature

`fn is_private_use(ch: char) -> bool`

# Called by

- [validate_codepoints](../../../../../functions/crates/lpe-domain/src/mailbox_name/validate_codepoints.md)