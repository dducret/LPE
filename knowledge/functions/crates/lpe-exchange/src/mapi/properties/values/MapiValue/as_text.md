---
type: Rust Method
title: as_text
resource: crates/lpe-exchange/src/mapi/properties/values.rs#L513-L519
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/compare_mapi_values
  - functions/crates/lpe-exchange/src/mapi/properties/folder/hierarchy_display_name
  - functions/crates/lpe-exchange/src/mapi/tables/pending/conversation_action_from_mapi_properties
---

# Signature

`pub(in crate::mapi) fn as_text(&self) -> Option<&str>`

# Called by

- [compare_mapi_values](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/compare_mapi_values.md)
- [hierarchy_display_name](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/folder/hierarchy_display_name.md)
- [conversation_action_from_mapi_properties](../../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/pending/conversation_action_from_mapi_properties.md)