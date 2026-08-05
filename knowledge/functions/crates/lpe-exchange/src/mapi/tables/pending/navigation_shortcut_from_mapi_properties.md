---
type: Rust Function
title: navigation_shortcut_from_mapi_properties
resource: crates/lpe-exchange/src/mapi/tables/pending.rs#L12-L132
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/pending/navigation_shortcut_folder_id_from_entry_id
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/identity/mapped_mapi_object_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/navigation_shortcut_save/validated_navigation_shortcut_from_mapi_properties
  - functions/crates/lpe-exchange/src/mapi/properties/navigation_shortcut/navigation_shortcut_with_pending_properties
  - functions/crates/lpe-exchange/src/mapi/properties/tests/microsoft_navigation_shortcut_example_preserves_wlink_properties
  - functions/crates/lpe-exchange/src/mapi/tables/pending/serialize_pending_navigation_shortcut_row
  - functions/crates/lpe-exchange/src/mapi/tables/tests/navigation_shortcut_parser_accepts_binary_wlink_group_ids
  - functions/crates/lpe-exchange/src/mapi/tables/tests/navigation_shortcut_parser_decodes_typed_and_wrapped_entry_id
---

# Signature

`pub(in crate::mapi) fn navigation_shortcut_from_mapi_properties( _account_id: Uuid, id: Option<Uuid>, properties: &HashMap<u32, MapiValue>, ) -> MapiNavigationShortcutMessage`

# Calls

- [navigation_shortcut_folder_id_from_entry_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/pending/navigation_shortcut_folder_id_from_entry_id.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [mapped_mapi_object_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/mapped_mapi_object_id.md)

# Called by

- [validated_navigation_shortcut_from_mapi_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/navigation_shortcut_save/validated_navigation_shortcut_from_mapi_properties.md)
- [navigation_shortcut_with_pending_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/navigation_shortcut/navigation_shortcut_with_pending_properties.md)
- [microsoft_navigation_shortcut_example_preserves_wlink_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/microsoft_navigation_shortcut_example_preserves_wlink_properties.md)
- [serialize_pending_navigation_shortcut_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/pending/serialize_pending_navigation_shortcut_row.md)
- [navigation_shortcut_parser_accepts_binary_wlink_group_ids](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/navigation_shortcut_parser_accepts_binary_wlink_group_ids.md)
- [navigation_shortcut_parser_decodes_typed_and_wrapped_entry_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/navigation_shortcut_parser_decodes_typed_and_wrapped_entry_id.md)