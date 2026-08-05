---
type: Rust Method
title: disconnect_ews_unified_messaging_call
resource: crates/lpe-exchange/src/tests/mod.rs#L5695-L5718
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/unified_messaging/ExchangeService/disconnect_phone_call
---

# Signature

`fn disconnect_ews_unified_messaging_call<'a>( &'a self, principal: &'a AccountPrincipal, call_id: &'a str, _audit: lpe_storage::AuditEntryInput, ) -> StoreFuture<'a, Option<EwsUnifiedMessagingCall>>`

# Called by

- [disconnect_phone_call](../../../../../../../functions/crates/lpe-exchange/src/service/ews/unified_messaging/ExchangeService/disconnect_phone_call.md)