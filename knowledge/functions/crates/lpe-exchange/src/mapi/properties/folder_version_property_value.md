---
type: Rust Function
title: folder_version_property_value
resource: crates/lpe-exchange/src/mapi/properties.rs#L605-L623
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/folder_properties_for_open_from_mailboxes
  - functions/crates/lpe-exchange/src/mapi/rop/serialize_session_folder_row
  - functions/crates/lpe-exchange/src/mapi/tables/diagnostics/outlook_bootstrap_row_invariant_summaries
  - functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_special_folder_row_with_version
  - functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_advertised_special_folder_row_with_counts_and_version
  - functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_folder_row_with_context_and_version
  - functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_collaboration_folder_row_with_context_and_version
---

# Signature

`pub(in crate::mapi) fn folder_version_property_value( version: &crate::mapi_store::MapiFolderVersion, property_tag: u32, ) -> Option<MapiValue>`

# Calls

- [canonical_property_storage_tag](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)

# Called by

- [folder_properties_for_open_from_mailboxes](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/folder_properties_for_open_from_mailboxes.md)
- [serialize_session_folder_row](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/serialize_session_folder_row.md)
- [outlook_bootstrap_row_invariant_summaries](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/diagnostics/outlook_bootstrap_row_invariant_summaries.md)
- [serialize_special_folder_row_with_version](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_special_folder_row_with_version.md)
- [serialize_advertised_special_folder_row_with_counts_and_version](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_advertised_special_folder_row_with_counts_and_version.md)
- [serialize_folder_row_with_context_and_version](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_folder_row_with_context_and_version.md)
- [serialize_collaboration_folder_row_with_context_and_version](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_collaboration_folder_row_with_context_and_version.md)