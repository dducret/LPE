---
type: Rust Function
title: bounded_search_criteria_to_rop
resource: crates/lpe-exchange/src/mapi/dispatch/search_folders.rs#L711-L792
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/rop_restriction_from_json_clause
  - functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/is_message_class_exclusion_clause
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/and_restriction
  - functions/crates/lpe-exchange/src/mapi/identity/mapped_mapi_object_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/append_get_search_criteria_response
---

# Signature

`pub(super) fn bounded_search_criteria_to_rop( definition: &lpe_storage::SearchFolderDefinition, mailboxes: &[JmapMailbox], use_unicode: bool, ) -> Result<(Vec<u8>, Vec<u64>, u32), u32>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [rop_restriction_from_json_clause](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/rop_restriction_from_json_clause.md)
- [is_message_class_exclusion_clause](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/is_message_class_exclusion_clause.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [and_restriction](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/and_restriction.md)
- [mapped_mapi_object_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/mapped_mapi_object_id.md)

# Called by

- [append_get_search_criteria_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/append_get_search_criteria_response.md)