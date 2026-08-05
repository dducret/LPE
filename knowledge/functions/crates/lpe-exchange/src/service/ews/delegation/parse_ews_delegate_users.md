---
type: Rust Function
title: parse_ews_delegate_users
resource: crates/lpe-exchange/src/service/ews/delegation.rs#L336-L349
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/element_contents
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/service/ews/delegation/parse_ews_delegate_user
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/delegation/ExchangeService/add_delegate
  - functions/crates/lpe-exchange/src/service/ews/delegation/ExchangeService/update_delegate
---

# Signature

`pub(in crate::service) fn parse_ews_delegate_users( principal: &AccountPrincipal, request: &str, delivery: &EwsDelegatePreferences, ) -> Result<Vec<UpsertEwsDelegateInput>>`

# Calls

- [element_contents](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_contents.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [parse_ews_delegate_user](../../../../../../../functions/crates/lpe-exchange/src/service/ews/delegation/parse_ews_delegate_user.md)

# Called by

- [add_delegate](../../../../../../../functions/crates/lpe-exchange/src/service/ews/delegation/ExchangeService/add_delegate.md)
- [update_delegate](../../../../../../../functions/crates/lpe-exchange/src/service/ews/delegation/ExchangeService/update_delegate.md)