---
type: Rust Function
title: compare_i64
resource: crates/lpe-exchange/src/mapi/properties.rs#L1304-L1306
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/compare_ordering
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_email_with_attachments
  - functions/crates/lpe-exchange/src/mapi/properties/restriction_matches
  - functions/crates/lpe-exchange/src/mapi/properties/compare_mapi_values
---

# Signature

`pub(in crate::mapi) fn compare_i64(left: i64, right: i64, relop: u8) -> bool`

# Calls

- [compare_ordering](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/compare_ordering.md)

# Called by

- [restriction_matches_email_with_attachments](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_email_with_attachments.md)
- [restriction_matches](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches.md)
- [compare_mapi_values](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/compare_mapi_values.md)