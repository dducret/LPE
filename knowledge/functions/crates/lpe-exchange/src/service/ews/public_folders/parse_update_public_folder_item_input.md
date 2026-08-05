---
type: Rust Function
title: parse_update_public_folder_item_input
resource: crates/lpe-exchange/src/service/ews/public_folders.rs#L78-L116
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/element_content
  - functions/crates/lpe-exchange/src/service/ews/fields/field_deleted
  - functions/crates/lpe-exchange/src/service/ews/xml/element_text
  - functions/crates/lpe-exchange/src/service/ews/xml/open_tag_text
  - functions/crates/lpe-exchange/src/service/ews/xml/attribute_value
  - functions/crates/lpe-exchange/src/service/ews/fields/deleted_or_updated_text
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/update_item
---

# Signature

`pub(in crate::service) fn parse_update_public_folder_item_input( principal: &AccountPrincipal, existing: &PublicFolderItem, request: &str, ) -> UpsertPublicFolderItemInput`

# Calls

- [element_content](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_content.md)
- [field_deleted](../../../../../../../functions/crates/lpe-exchange/src/service/ews/fields/field_deleted.md)
- [element_text](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_text.md)
- [open_tag_text](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/open_tag_text.md)
- [attribute_value](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/attribute_value.md)
- [deleted_or_updated_text](../../../../../../../functions/crates/lpe-exchange/src/service/ews/fields/deleted_or_updated_text.md)

# Called by

- [update_item](../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/update_item.md)