---
type: Rust Method
title: update_inbox_rules
resource: crates/lpe-exchange/src/service/ews/rules.rs#L16-L101
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/element_contents
  - functions/crates/lpe-exchange/src/service/ews/xml/element_text
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/service/ews/xml/element_content
  - functions/crates/lpe-exchange/src/service/ews/rules/bounded_ews_rule_to_sieve
  - functions/crates/lpe-exchange/src/service/ews/responses/simple_operation_success_response
  - functions/crates/lpe-exchange/src/service/ews/responses/operation_error_response
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle
---

# Signature

`pub(in crate::service) async fn update_inbox_rules( &self, principal: &AccountPrincipal, request: &str, ) -> Result<String>`

# Calls

- [element_contents](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_contents.md)
- [element_text](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_text.md)
- [push](../../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [element_content](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_content.md)
- [bounded_ews_rule_to_sieve](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/rules/bounded_ews_rule_to_sieve.md)
- [simple_operation_success_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/responses/simple_operation_success_response.md)
- [operation_error_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/responses/operation_error_response.md)

# Called by

- [handle](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle.md)