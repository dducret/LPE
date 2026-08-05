---
type: Rust Function
title: compare_mapi_values
resource: crates/lpe-exchange/src/mapi/properties.rs#L1256-L1270
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/values/MapiValue/as_i64
  - functions/crates/lpe-exchange/src/mapi/properties/compare_i64
  - functions/crates/lpe-exchange/src/mapi/properties/values/MapiValue/as_text
  - functions/crates/lpe-exchange/src/mapi/properties/compare_ordering
  - functions/crates/lpe-exchange/src/mapi/tables/sorting/compare_case_insensitive
  - functions/crates/lpe-exchange/src/mapi/properties/values/MapiValue/as_bool
  - functions/crates/lpe-exchange/src/mapi/properties/compare_folder_entry_id_values
  - functions/crates/lpe-exchange/src/mapi/properties/values/MapiValue/cmp_value
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_email_with_attachments
  - functions/crates/lpe-exchange/src/mapi/properties/restriction_matches
---

# Signature

`pub(in crate::mapi) fn compare_mapi_values(left: &MapiValue, right: &MapiValue, relop: u8) -> bool`

# Calls

- [as_i64](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/MapiValue/as_i64.md)
- [compare_i64](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/compare_i64.md)
- [as_text](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/MapiValue/as_text.md)
- [compare_ordering](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/compare_ordering.md)
- [compare_case_insensitive](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/sorting/compare_case_insensitive.md)
- [as_bool](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/MapiValue/as_bool.md)
- [compare_folder_entry_id_values](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/compare_folder_entry_id_values.md)
- [cmp_value](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/MapiValue/cmp_value.md)

# Called by

- [restriction_matches_email_with_attachments](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_email_with_attachments.md)
- [restriction_matches](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches.md)