---
type: Rust Function
title: summarize_umolk_roaming_dictionary_contract
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses.rs#L446-L456
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/extract_getprops_binary_value
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/summarize_umolk_roaming_dictionary_xml
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/record_outlook_umolk_getprops_materialization
---

# Signature

`fn summarize_umolk_roaming_dictionary_contract( property_tags: &[u32], response: &[u8], ) -> UmolkRoamingDictionaryContractSummary`

# Calls

- [extract_getprops_binary_value](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/extract_getprops_binary_value.md)
- [summarize_umolk_roaming_dictionary_xml](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/summarize_umolk_roaming_dictionary_xml.md)

# Called by

- [record_outlook_umolk_getprops_materialization](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/record_outlook_umolk_getprops_materialization.md)