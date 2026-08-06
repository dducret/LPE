---
type: Rust Method
title: handle_canonical_unsupported_write
resource: crates/lpe-jmap/src/service/canonical.rs#L403-L457
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/service/helpers/requested_account_id_from_arguments
  - functions/crates/lpe-jmap/src/service/canonical/JmapService/canonical_object_state
  - functions/crates/lpe-jmap/src/service/helpers/canonical_create_ids
  - functions/crates/lpe-jmap/src/service/helpers/object_keys
  - functions/crates/lpe-jmap/src/service/helpers/string_ids_from_arguments
  called_by:
  - functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account
  - functions/crates/lpe-jmap/src/service/canonical/JmapService/handle_canonical_import_or_copy
---

# Signature

`pub(crate) async fn handle_canonical_unsupported_write( &self, account: &AuthenticatedAccount, arguments: Value, data_type: &str, method_name: &str, ) -> Result<Value>`

# Calls

- [requested_account_id_from_arguments](../../../../../../../functions/crates/lpe-jmap/src/service/helpers/requested_account_id_from_arguments.md)
- [canonical_object_state](../../../../../../../functions/crates/lpe-jmap/src/service/canonical/JmapService/canonical_object_state.md)
- [canonical_create_ids](../../../../../../../functions/crates/lpe-jmap/src/service/helpers/canonical_create_ids.md)
- [object_keys](../../../../../../../functions/crates/lpe-jmap/src/service/helpers/object_keys.md)
- [string_ids_from_arguments](../../../../../../../functions/crates/lpe-jmap/src/service/helpers/string_ids_from_arguments.md)

# Called by

- [handle_api_request_for_account](../../../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account.md)
- [handle_canonical_import_or_copy](../../../../../../../functions/crates/lpe-jmap/src/service/canonical/JmapService/handle_canonical_import_or_copy.md)