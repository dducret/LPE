---
type: Rust Function
title: resolve_result_references
resource: crates/lpe-jmap/src/service/helpers.rs#L454-L512
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/service/helpers/result_reference_error
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/LPE-CT/web/app/smoke/test/MockClassList/remove
  called_by:
  - functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account
---

# Signature

`pub(super) fn resolve_result_references( arguments: Value, previous_results: &HashMap<String, (String, Value)>, ) -> std::result::Result<Value, Value>`

# Calls

- [result_reference_error](../../../../../../functions/crates/lpe-jmap/src/service/helpers/result_reference_error.md)
- [get](../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [remove](../../../../../../functions/LPE-CT/web/app/smoke/test/MockClassList/remove.md)

# Called by

- [handle_api_request_for_account](../../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account.md)