---
type: Rust Function
title: search_content_row_matches
resource: crates/lpe-exchange/src/mapi/tables/search_folders.rs#L51-L61
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_email
  - functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_task
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response
---

# Signature

`pub(super) fn search_content_row_matches( row: &SearchContentRow<'_>, restriction: Option<&MapiRestriction>, ) -> bool`

# Calls

- [restriction_matches_email](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_email.md)
- [restriction_matches_task](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_task.md)

# Called by

- [rop_find_row_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response.md)