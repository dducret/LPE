---
type: Rust Function
title: modify_rules_response
resource: crates/lpe-exchange/src/tests/mapi_over_http.rs#L332-L383
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/mapi_headers
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
  - functions/crates/lpe-exchange/src/tests/mapi_cookie_header
  - functions/crates/lpe-exchange/src/tests/append_rop_open_folder
  - functions/crates/lpe-exchange/src/tests/test_mapi_folder_id
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/tests/append_mapi_utf16_property
  - functions/crates/lpe-exchange/src/tests/execute_body
  - functions/crates/lpe-exchange/src/tests/response_rops_from_execute_response
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_modify_rules_accepts_bounded_sieve_actions
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_modify_rules_rejects_exchange_rule_blobs
---

# Signature

`async fn modify_rules_response( name: &str, provider_data: serde_json::Value, ) -> (Vec<u8>, Option<String>)`

# Calls

- [mapi_headers](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_headers.md)
- [from_str](../../../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)
- [mapi_cookie_header](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_cookie_header.md)
- [append_rop_open_folder](../../../../../../functions/crates/lpe-exchange/src/tests/append_rop_open_folder.md)
- [test_mapi_folder_id](../../../../../../functions/crates/lpe-exchange/src/tests/test_mapi_folder_id.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [append_mapi_utf16_property](../../../../../../functions/crates/lpe-exchange/src/tests/append_mapi_utf16_property.md)
- [execute_body](../../../../../../functions/crates/lpe-exchange/src/tests/execute_body.md)
- [response_rops_from_execute_response](../../../../../../functions/crates/lpe-exchange/src/tests/response_rops_from_execute_response.md)

# Called by

- [mapi_over_http_modify_rules_accepts_bounded_sieve_actions](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_modify_rules_accepts_bounded_sieve_actions.md)
- [mapi_over_http_modify_rules_rejects_exchange_rule_blobs](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_modify_rules_rejects_exchange_rule_blobs.md)