---
type: Rust Function
title: journal_entry_named_property_value
resource: crates/lpe-exchange/src/mapi/properties/notes.rs#L124-L162
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_rfc3339_utc
  - functions/crates/lpe-exchange/src/mapi/properties/notes/json_string_array
  - functions/crates/lpe-exchange/src/mapi/properties/notes/empty_contact_link_entry_blob
  - functions/crates/lpe-exchange/src/mapi/properties/notes/empty_contact_link_search_key_blob
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/notes/journal_entry_property_value
---

# Signature

`pub(in crate::mapi) fn journal_entry_named_property_value( entry: &JournalEntry, property_tag: u32, ) -> Option<MapiValue>`

# Calls

- [filetime_from_rfc3339_utc](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_rfc3339_utc.md)
- [json_string_array](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/notes/json_string_array.md)
- [empty_contact_link_entry_blob](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/notes/empty_contact_link_entry_blob.md)
- [empty_contact_link_search_key_blob](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/notes/empty_contact_link_search_key_blob.md)

# Called by

- [journal_entry_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/notes/journal_entry_property_value.md)