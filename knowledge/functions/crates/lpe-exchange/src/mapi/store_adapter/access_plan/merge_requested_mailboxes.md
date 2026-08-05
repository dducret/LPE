---
type: Rust Function
title: merge_requested_mailboxes
resource: crates/lpe-exchange/src/mapi/store_adapter/access_plan.rs#L996-L1012
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_store_for_access_plan
  - functions/crates/lpe-exchange/src/mapi/store_adapter/tests/merge_requested_mailboxes_adds_custom_identity_rows
---

# Signature

`pub(in crate::mapi) fn merge_requested_mailboxes( mailboxes: &mut Vec<JmapMailbox>, all_mailboxes: &[JmapMailbox], requested_mailbox_ids: &[Uuid], )`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [load_mapi_store_for_access_plan](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_store_for_access_plan.md)
- [merge_requested_mailboxes_adds_custom_identity_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/merge_requested_mailboxes_adds_custom_identity_rows.md)