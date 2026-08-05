---
type: Rust Method
title: handle_search_folder_set
resource: crates/lpe-jmap/src/service.rs#L1285-L1371
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/service/helpers/requested_account_id_from_arguments
  - functions/crates/lpe-jmap/src/service/canonical/JmapService/canonical_object_state
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-jmap/src/service/helpers/search_folder_input_from_value
  - functions/crates/lpe-jmap/src/error/set_error
  - functions/crates/lpe-jmap/src/parse/parse_uuid
  - functions/crates/lpe-jmap/src/service/helpers/string_ids_from_arguments
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account
  - functions/crates/lpe-jmap/src/service/JmapService/handle_search_folder_import_or_copy
---

# Signature

`pub(crate) async fn handle_search_folder_set( &self, account: &AuthenticatedAccount, arguments: Value, created_ids: &mut HashMap<String, String>, ) -> Result<Value>`

# Calls

- [requested_account_id_from_arguments](../../../../../../functions/crates/lpe-jmap/src/service/helpers/requested_account_id_from_arguments.md)
- [canonical_object_state](../../../../../../functions/crates/lpe-jmap/src/service/canonical/JmapService/canonical_object_state.md)
- [get](../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [search_folder_input_from_value](../../../../../../functions/crates/lpe-jmap/src/service/helpers/search_folder_input_from_value.md)
- [set_error](../../../../../../functions/crates/lpe-jmap/src/error/set_error.md)
- [parse_uuid](../../../../../../functions/crates/lpe-jmap/src/parse/parse_uuid.md)
- [string_ids_from_arguments](../../../../../../functions/crates/lpe-jmap/src/service/helpers/string_ids_from_arguments.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [handle_api_request_for_account](../../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account.md)
- [handle_search_folder_import_or_copy](../../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_search_folder_import_or_copy.md)