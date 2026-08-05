---
type: Rust Function
title: find_hierarchy_row
resource: crates/lpe-exchange/src/mapi/tables/find.rs#L41-L72
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/find_origin
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/find_backward
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_row_matches
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response
---

# Signature

`pub(super) fn find_hierarchy_row<'a>( rows: &'a [HierarchyRow<'a>], mailboxes: &[JmapMailbox], current_position: usize, request: &RopRequest, restriction: Option<&MapiRestriction>, mailbox_guid: Uuid, ) -> Option<(usize, HierarchyRow<'a>)>`

# Calls

- [find_origin](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/find_origin.md)
- [find_backward](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/find_backward.md)
- [hierarchy_row_matches](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_row_matches.md)

# Called by

- [rop_find_row_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response.md)