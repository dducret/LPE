---
type: Rust Function
title: restriction_matches_conversation_member_in_snapshot
resource: crates/lpe-exchange/src/mapi/tables/filters.rs#L18-L29
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/filters/restriction_matches_email_in_snapshot
  - functions/crates/lpe-exchange/src/mapi/tables/folders/mapi_folder_id_for_email
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/counts/table_position_and_count
  - functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner
---

# Signature

`pub(super) fn restriction_matches_conversation_member_in_snapshot( restriction: Option<&MapiRestriction>, email: &JmapEmail, snapshot: &MapiMailStoreSnapshot, ) -> bool`

# Calls

- [restriction_matches_email_in_snapshot](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/filters/restriction_matches_email_in_snapshot.md)
- [mapi_folder_id_for_email](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/mapi_folder_id_for_email.md)

# Called by

- [table_position_and_count](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/counts/table_position_and_count.md)
- [rop_query_rows_response_inner](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner.md)