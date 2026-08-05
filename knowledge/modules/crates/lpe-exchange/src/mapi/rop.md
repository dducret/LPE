---
type: Rust Module
title: rop
resource: crates/lpe-exchange/src/mapi/rop.rs#L1-L1615
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/super-properties
  - external/super-session
  - external/super-sync
  - external/super-tables
  - external/super-wire-mapierror-mapipropertytype-mapirestrictiontype-ropid
  - external/super
  - external/pub-in-crate-mapi-use-attachments
  - external/pub-in-crate-mapi-use-buffer
  - external/pub-in-crate-mapi-use-debug
  - external/pub-in-crate-mapi-use-errors
  - external/pub-in-crate-mapi-use-event-properties
  - external/pub-in-crate-mapi-use-logon
  - external/pub-in-crate-mapi-use-named-properties
  - external/pub-in-crate-mapi-use-object-ids
  - external/pub-in-crate-mapi-use-parse
  - external/property-limits
  - external/pub-in-crate-mapi-use-receive-folders
  - external/pub-in-crate-mapi-use-recipients
  - external/pub-in-crate-mapi-use-request-reader
  - external/pub-in-crate-mapi-use-responses
  - external/pub-in-crate-mapi-use-restrictions
  - external/pub-in-crate-mapi-use-serialize
  - external/pub-in-crate-mapi-use-typed-requests
  member_of:
  - packages/crates/lpe-exchange
---

# Contains

- [rop_get_properties_specific_response](../../../../../functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_specific_response.md)
- [rop_get_properties_specific_response_with_custom](../../../../../functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_specific_response_with_custom.md)
- [serialize_object_property_row_with_custom](../../../../../functions/crates/lpe-exchange/src/mapi/rop/serialize_object_property_row_with_custom.md)
- [unsupported_specific_property_tags](../../../../../functions/crates/lpe-exchange/src/mapi/rop/unsupported_specific_property_tags.md)
- [fallback_default_specific_property](../../../../../functions/crates/lpe-exchange/src/mapi/rop/fallback_default_specific_property.md)
- [associated_config_modeled_property](../../../../../functions/crates/lpe-exchange/src/mapi/rop/associated_config_modeled_property.md)
- [flagged_property_error_code](../../../../../functions/crates/lpe-exchange/src/mapi/rop/flagged_property_error_code.md)
- [write_flagged_property_row](../../../../../functions/crates/lpe-exchange/src/mapi/rop/write_flagged_property_row.md)
- [write_flagged_property_error](../../../../../functions/crates/lpe-exchange/src/mapi/rop/write_flagged_property_error.md)
- [utf16le_bytes](../../../../../functions/crates/lpe-exchange/src/mapi/rop/utf16le_bytes.md)
- [property_is_unsupported_for_object](../../../../../functions/crates/lpe-exchange/src/mapi/rop/property_is_unsupported_for_object.md)
- [get_properties_specific_value_tag](../../../../../functions/crates/lpe-exchange/src/mapi/rop/get_properties_specific_value_tag.md)
- [get_properties_specific_typed_value_tag](../../../../../functions/crates/lpe-exchange/src/mapi/rop/get_properties_specific_typed_value_tag.md)
- [get_properties_specific_candidate_tags](../../../../../functions/crates/lpe-exchange/src/mapi/rop/get_properties_specific_candidate_tags.md)
- [modeled_zero_or_default_property](../../../../../functions/crates/lpe-exchange/src/mapi/rop/modeled_zero_or_default_property.md)
- [is_modeled_empty_special_folder_class_property](../../../../../functions/crates/lpe-exchange/src/mapi/rop/is_modeled_empty_special_folder_class_property.md)
- [rop_get_properties_all_response](../../../../../functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_all_response.md)
- [request_get_properties_all_want_unicode](../../../../../functions/crates/lpe-exchange/src/mapi/rop/request_get_properties_all_want_unicode.md)
- [get_properties_all_response_tag](../../../../../functions/crates/lpe-exchange/src/mapi/rop/get_properties_all_response_tag.md)
- [property_error_tag](../../../../../functions/crates/lpe-exchange/src/mapi/rop/property_error_tag.md)
- [default_folder_property_tags_with_identity](../../../../../functions/crates/lpe-exchange/src/mapi/rop/default_folder_property_tags_with_identity.md)
- [serialize_object_property](../../../../../functions/crates/lpe-exchange/src/mapi/rop/serialize_object_property.md)
- [serialize_session_folder_row](../../../../../functions/crates/lpe-exchange/src/mapi/rop/serialize_session_folder_row.md)
- [folder_row_for_id](../../../../../functions/crates/lpe-exchange/src/mapi/rop/folder_row_for_id.md)

# Imports

- `super::properties::*`
- `super::session::*`
- `super::sync::*`
- `super::tables::*`
- `super::wire::{MapiError, MapiPropertyType, MapiRestrictionType, RopId}`
- `super::*`
- `pub(in crate::mapi) use attachments::*`
- `pub(in crate::mapi) use buffer::*`
- `pub(in crate::mapi) use debug::*`
- `pub(in crate::mapi) use errors::*`
- `pub(in crate::mapi) use event_properties::*`
- `pub(in crate::mapi) use logon::*`
- `pub(in crate::mapi) use named_properties::*`
- `pub(in crate::mapi) use object_ids::*`
- `pub(in crate::mapi) use parse::*`
- `property_limits::*`
- `pub(in crate::mapi) use receive_folders::*`
- `pub(in crate::mapi) use recipients::*`
- `pub(in crate::mapi) use request_reader::*`
- `pub(in crate::mapi) use responses::*`
- `pub(in crate::mapi) use restrictions::*`
- `pub(in crate::mapi) use serialize::*`
- `pub(in crate::mapi) use typed_requests::*`

# Member of

- [lpe-exchange](../../../../../packages/crates/lpe-exchange.md)