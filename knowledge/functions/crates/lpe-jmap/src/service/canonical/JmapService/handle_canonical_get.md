---
type: Rust Method
title: handle_canonical_get
resource: crates/lpe-jmap/src/service/canonical.rs#L4-L50
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/service/helpers/requested_account_id_from_arguments
  - functions/crates/lpe-jmap/src/service/helpers/string_ids_from_arguments
  - functions/crates/lpe-jmap/src/service/helpers/property_names_from_arguments
  - functions/crates/lpe-jmap/src/service/canonical/JmapService/canonical_objects
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-jmap/src/service/helpers/project_get_properties
  called_by:
  - functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account
---

# Signature

`pub(crate) async fn handle_canonical_get( &self, account: &AuthenticatedAccount, arguments: Value, data_type: &str, ) -> Result<Value>`

# Calls

- [requested_account_id_from_arguments](../../../../../../../functions/crates/lpe-jmap/src/service/helpers/requested_account_id_from_arguments.md)
- [string_ids_from_arguments](../../../../../../../functions/crates/lpe-jmap/src/service/helpers/string_ids_from_arguments.md)
- [property_names_from_arguments](../../../../../../../functions/crates/lpe-jmap/src/service/helpers/property_names_from_arguments.md)
- [canonical_objects](../../../../../../../functions/crates/lpe-jmap/src/service/canonical/JmapService/canonical_objects.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [project_get_properties](../../../../../../../functions/crates/lpe-jmap/src/service/helpers/project_get_properties.md)

# Called by

- [handle_api_request_for_account](../../../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account.md)