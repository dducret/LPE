---
type: Rust Function
title: message_matches_set
resource: crates/lpe-imap/src/search.rs#L117-L155
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-imap/src/search/SearchExpression/matches
---

# Signature

`fn message_matches_set( email: &ImapEmail, index: usize, emails: &[ImapEmail], set_token: &str, ref_kind: MessageRefKind, ) -> Result<bool>`

# Called by

- [matches](../../../../../functions/crates/lpe-imap/src/search/SearchExpression/matches.md)