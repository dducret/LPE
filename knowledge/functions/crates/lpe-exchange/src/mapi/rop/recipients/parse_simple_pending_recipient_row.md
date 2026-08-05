---
type: Rust Function
title: parse_simple_pending_recipient_row
resource: crates/lpe-exchange/src/mapi/rop/recipients.rs#L96-L134
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  - functions/crates/lpe-exchange/src/mapi/rop/parse/parse_property_value_for_tag
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from
  - functions/crates/lpe-exchange/src/mapi/rop/recipients/normalize_recipient_type
  - functions/crates/lpe-exchange/src/mapi/rop/recipients/optional_mapi_value_text
  - functions/crates/lpe-exchange/src/mapi/rop/recipients/recipient_display_name_from_values
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/recipients/parse_pending_recipient_row
---

# Signature

`fn parse_simple_pending_recipient_row( row_id: u32, fallback_recipient_type: u8, columns: &[u32], row: &[u8], ) -> Result<PendingRecipient>`

# Calls

- [canonical_property_storage_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)
- [parse_property_value_for_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/parse_property_value_for_tag.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [try_from](../../../../../../../functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from.md)
- [normalize_recipient_type](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/recipients/normalize_recipient_type.md)
- [optional_mapi_value_text](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/recipients/optional_mapi_value_text.md)
- [recipient_display_name_from_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/recipients/recipient_display_name_from_values.md)

# Called by

- [parse_pending_recipient_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/recipients/parse_pending_recipient_row.md)