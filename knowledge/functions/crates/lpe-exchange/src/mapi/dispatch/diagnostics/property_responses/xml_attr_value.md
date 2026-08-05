---
type: Rust Function
title: xml_attr_value
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses.rs#L541-L552
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/summarize_umolk_roaming_dictionary_xml
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/xml_element_attr_by_key
---

# Signature

`fn xml_attr_value(text: &str, element: &str, attr: &str) -> Option<String>`

# Called by

- [summarize_umolk_roaming_dictionary_xml](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/summarize_umolk_roaming_dictionary_xml.md)
- [xml_element_attr_by_key](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/xml_element_attr_by_key.md)