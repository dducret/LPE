---
type: Rust Function
title: hierarchy_row_property_value
resource: crates/lpe-exchange/src/mapi/tables/hierarchy.rs#L463-L487
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_row_folder_flags
  - functions/crates/lpe-exchange/src/mapi/properties/mailbox_property_value_with_context_for_account
  - functions/crates/lpe-exchange/src/mapi/properties/public_folder_property_value
  - functions/crates/lpe-exchange/src/mapi/properties/collaboration_folder_property_value
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/special_folder_property_value
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/diagnostics/outlook_bootstrap_row_invariant_summaries
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_row_property_is_present
---

# Signature

`pub(super) fn hierarchy_row_property_value( row: &HierarchyRow<'_>, mailboxes: &[JmapMailbox], property_tag: u32, mailbox_guid: Uuid, ) -> Option<MapiValue>`

# Calls

- [canonical_property_storage_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)
- [hierarchy_row_folder_flags](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_row_folder_flags.md)
- [mailbox_property_value_with_context_for_account](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/mailbox_property_value_with_context_for_account.md)
- [public_folder_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/public_folder_property_value.md)
- [collaboration_folder_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/collaboration_folder_property_value.md)
- [special_folder_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/special_folder_property_value.md)

# Called by

- [outlook_bootstrap_row_invariant_summaries](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/diagnostics/outlook_bootstrap_row_invariant_summaries.md)
- [hierarchy_row_property_is_present](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_row_property_is_present.md)