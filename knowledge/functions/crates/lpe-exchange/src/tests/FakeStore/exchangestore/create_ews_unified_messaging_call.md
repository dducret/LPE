---
type: Rust Method
title: create_ews_unified_messaging_call
resource: crates/lpe-exchange/src/tests/mod.rs#L5775-L5799
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/unified_messaging/ExchangeService/play_on_phone
---

# Signature

`fn create_ews_unified_messaging_call<'a>( &'a self, principal: &'a AccountPrincipal, phone_number: Option<&'a str>, message_id: Option<Uuid>, _audit: lpe_storage::AuditEntryInput, ) -> StoreFuture<'a, EwsUnifiedMessagingCall>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [play_on_phone](../../../../../../../functions/crates/lpe-exchange/src/service/ews/unified_messaging/ExchangeService/play_on_phone.md)