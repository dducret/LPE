---
type: Rust Function
title: summarize_flagged_getprops_materialization
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses.rs#L575-L631
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_u8
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/rop/parse/parse_property_value_for_tag
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/record_outlook_umolk_getprops_materialization
---

# Signature

`fn summarize_flagged_getprops_materialization( property_tags: &[u32], response: &[u8], ) -> GetPropsMaterializationSummary`

# Calls

- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [read_u8](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_u8.md)
- [push](../../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [parse_property_value_for_tag](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/parse_property_value_for_tag.md)

# Called by

- [record_outlook_umolk_getprops_materialization](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/record_outlook_umolk_getprops_materialization.md)