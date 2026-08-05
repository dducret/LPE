---
type: Rust Function
title: summarize_umolk_roaming_dictionary_xml
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses.rs#L490-L511
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/xml_attr_value
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/xml_element_attr_by_key
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/sanitize_debug_token
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/classify_olprefs_version_value
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/summarize_umolk_roaming_dictionary_contract
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/umolk_dictionary_contract_classifies_positive_olprefs_version
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/umolk_dictionary_contract_classifies_zero_olprefs_version
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/umolk_dictionary_contract_reports_missing_olprefs_version
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/umolk_dictionary_contract_reports_invalid_olprefs_version
---

# Signature

`fn summarize_umolk_roaming_dictionary_xml(bytes: &[u8]) -> UmolkRoamingDictionaryContractSummary`

# Calls

- [xml_attr_value](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/xml_attr_value.md)
- [xml_element_attr_by_key](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/xml_element_attr_by_key.md)
- [sanitize_debug_token](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/sanitize_debug_token.md)
- [classify_olprefs_version_value](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/classify_olprefs_version_value.md)

# Called by

- [summarize_umolk_roaming_dictionary_contract](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/summarize_umolk_roaming_dictionary_contract.md)
- [umolk_dictionary_contract_classifies_positive_olprefs_version](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/umolk_dictionary_contract_classifies_positive_olprefs_version.md)
- [umolk_dictionary_contract_classifies_zero_olprefs_version](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/umolk_dictionary_contract_classifies_zero_olprefs_version.md)
- [umolk_dictionary_contract_reports_missing_olprefs_version](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/umolk_dictionary_contract_reports_missing_olprefs_version.md)
- [umolk_dictionary_contract_reports_invalid_olprefs_version](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/umolk_dictionary_contract_reports_invalid_olprefs_version.md)