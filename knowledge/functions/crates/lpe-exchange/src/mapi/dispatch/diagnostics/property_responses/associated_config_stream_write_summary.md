---
type: Rust Function
title: associated_config_stream_write_summary
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses.rs#L670-L689
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/probes/set_properties_probe_request
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/associated_config_stream_write_summary_names_roaming_xml
---

# Signature

`pub(in crate::mapi::dispatch) fn associated_config_stream_write_summary( values: &[(u32, MapiValue)], ) -> String`

# Calls

- [canonical_property_storage_tag](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)
- [push](../../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [set_properties_probe_request](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/probes/set_properties_probe_request.md)
- [associated_config_stream_write_summary_names_roaming_xml](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/associated_config_stream_write_summary_names_roaming_xml.md)