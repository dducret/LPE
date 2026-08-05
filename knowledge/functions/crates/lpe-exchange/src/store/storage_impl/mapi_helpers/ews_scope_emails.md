---
type: Rust Function
title: ews_scope_emails
resource: crates/lpe-exchange/src/store/storage_impl/mapi_helpers.rs#L16-L28
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
---

# Signature

`fn ews_scope_emails(principal: &AccountPrincipal, mailbox_emails: &[String]) -> Vec<String>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)