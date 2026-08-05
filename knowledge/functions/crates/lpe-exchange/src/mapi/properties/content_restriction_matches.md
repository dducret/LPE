---
type: Rust Function
title: content_restriction_matches
resource: crates/lpe-exchange/src/mapi/properties.rs#L469-L487
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_email_with_attachments
  - functions/crates/lpe-exchange/src/mapi/properties/restriction_matches
---

# Signature

`fn content_restriction_matches( property: &str, value: &str, fuzzy_level_low: u16, fuzzy_level_high: u16, ) -> bool`

# Called by

- [restriction_matches_email_with_attachments](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_email_with_attachments.md)
- [restriction_matches](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches.md)