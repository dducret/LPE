---
type: Rust Function
title: list_pattern_match_from
resource: crates/lpe-domain/src/mailbox_name.rs#L448-L475
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/position
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-domain/src/mailbox_name/MailboxNamePolicy/list_pattern_matches
---

# Signature

`fn list_pattern_match_from( name: &[char], pattern: &[char], name_index: usize, pattern_index: usize, ) -> bool`

# Calls

- [position](../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/position.md)
- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [list_pattern_matches](../../../../../functions/crates/lpe-domain/src/mailbox_name/MailboxNamePolicy/list_pattern_matches.md)