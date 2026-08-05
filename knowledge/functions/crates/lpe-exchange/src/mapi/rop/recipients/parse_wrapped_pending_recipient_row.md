---
type: Rust Function
title: parse_wrapped_pending_recipient_row
resource: crates/lpe-exchange/src/mapi/rop/recipients.rs#L136-L255
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_u16
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_u8
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_ascii_z
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_bytes
  - functions/crates/lpe-exchange/src/mapi/rop/recipients/read_recipient_string
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  - functions/crates/lpe-exchange/src/mapi/rop/parse/parse_property_value_for_tag
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from
  - functions/crates/lpe-exchange/src/mapi/rop/recipients/normalize_recipient_type
  - functions/crates/lpe-exchange/src/mapi/rop/recipients/optional_mapi_value_text
  - functions/crates/lpe-exchange/src/mapi/rop/recipients/legacy_dn_recipient_address
  - functions/crates/lpe-exchange/src/mapi/rop/recipients/recipient_display_name_from_values
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/recipients/parse_pending_recipient_row
---

# Signature

`fn parse_wrapped_pending_recipient_row( row_id: u32, fallback_recipient_type: u8, columns: &[u32], row: &[u8], principal: &AccountPrincipal, address_book_entries: &[ExchangeAddressBookEntry], ) -> Result<PendingRecipient>`

# Calls

- [read_u16](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_u16.md)
- [read_u8](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_u8.md)
- [read_ascii_z](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_ascii_z.md)
- [read_bytes](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_bytes.md)
- [read_recipient_string](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/recipients/read_recipient_string.md)
- [canonical_property_storage_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)
- [parse_property_value_for_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/parse_property_value_for_tag.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [try_from](../../../../../../../functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from.md)
- [normalize_recipient_type](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/recipients/normalize_recipient_type.md)
- [optional_mapi_value_text](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/recipients/optional_mapi_value_text.md)
- [legacy_dn_recipient_address](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/recipients/legacy_dn_recipient_address.md)
- [recipient_display_name_from_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/recipients/recipient_display_name_from_values.md)

# Called by

- [parse_pending_recipient_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/recipients/parse_pending_recipient_row.md)