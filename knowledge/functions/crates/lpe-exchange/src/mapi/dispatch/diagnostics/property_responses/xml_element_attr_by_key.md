---
type: Rust Function
title: xml_element_attr_by_key
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses.rs#L524-L539
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/xml_attr_value
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/summarize_umolk_roaming_dictionary_xml
---

# Signature

`fn xml_element_attr_by_key(text: &str, element: &str, key: &str, attr: &str) -> Option<String>`

# Calls

- [xml_attr_value](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/xml_attr_value.md)

# Called by

- [summarize_umolk_roaming_dictionary_xml](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/summarize_umolk_roaming_dictionary_xml.md)