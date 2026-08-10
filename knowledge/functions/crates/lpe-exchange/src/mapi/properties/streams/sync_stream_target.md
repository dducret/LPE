---
type: Rust Function
title: sync_stream_target
resource: crates/lpe-exchange/src/mapi/properties/streams.rs#L665-L800
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/streams/stream_property_value
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  - functions/LPE-CT/web/app/smoke/test/MockClassList/remove
  - functions/crates/lpe-exchange/src/mapi_store/associated_config/is_associated_config_read_only_property_tag
  - functions/crates/lpe-exchange/src/mapi/properties/values/mapi_properties_from_json
  - functions/crates/lpe-exchange/src/mapi/properties/values/mapi_properties_to_json
  - functions/crates/lpe-exchange/src/mapi_store/associated_config/copy_associated_config_server_metadata
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/streams/write_stream
  - functions/crates/lpe-exchange/src/mapi/properties/streams/set_attachment_stream_size
---

# Signature

`pub(in crate::mapi) fn sync_stream_target( session: &mut MapiSession, target: StreamWriteTarget, data: Vec<u8>, ) -> Option<()>`

# Calls

- [stream_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/stream_property_value.md)
- [canonical_property_storage_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)
- [remove](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockClassList/remove.md)
- [is_associated_config_read_only_property_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/is_associated_config_read_only_property_tag.md)
- [mapi_properties_from_json](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/mapi_properties_from_json.md)
- [mapi_properties_to_json](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/mapi_properties_to_json.md)
- [copy_associated_config_server_metadata](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/copy_associated_config_server_metadata.md)

# Called by

- [write_stream](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/write_stream.md)
- [set_attachment_stream_size](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/set_attachment_stream_size.md)