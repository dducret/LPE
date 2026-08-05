---
type: Rust Method
title: handle_canonical_changes
resource: crates/lpe-jmap/src/service/canonical.rs#L219-L256
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/service/helpers/requested_account_id_from_arguments
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-jmap/src/service/canonical/JmapService/canonical_objects
  - functions/crates/lpe-jmap/src/service/helpers/opaque_state_fingerprint
  - functions/crates/lpe-jmap/src/service/canonical/JmapService/string_object_changes_response
  - functions/crates/lpe-jmap/src/service/object_state/JmapService/object_changes_response
  called_by:
  - functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account
---

# Signature

`pub(crate) async fn handle_canonical_changes( &self, account: &AuthenticatedAccount, arguments: Value, data_type: &str, ) -> Result<Value>`

# Calls

- [requested_account_id_from_arguments](../../../../../../../functions/crates/lpe-jmap/src/service/helpers/requested_account_id_from_arguments.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [canonical_objects](../../../../../../../functions/crates/lpe-jmap/src/service/canonical/JmapService/canonical_objects.md)
- [opaque_state_fingerprint](../../../../../../../functions/crates/lpe-jmap/src/service/helpers/opaque_state_fingerprint.md)
- [string_object_changes_response](../../../../../../../functions/crates/lpe-jmap/src/service/canonical/JmapService/string_object_changes_response.md)
- [object_changes_response](../../../../../../../functions/crates/lpe-jmap/src/service/object_state/JmapService/object_changes_response.md)

# Called by

- [handle_api_request_for_account](../../../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account.md)