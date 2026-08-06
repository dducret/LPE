---
type: Rust Method
title: fetch_ews_unified_messaging_call
resource: crates/lpe-exchange/src/tests/mod.rs#L5801-L5818
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/unified_messaging/ExchangeService/get_phone_call_information
---

# Signature

`fn fetch_ews_unified_messaging_call<'a>( &'a self, principal: &'a AccountPrincipal, call_id: &'a str, ) -> StoreFuture<'a, Option<EwsUnifiedMessagingCall>>`

# Called by

- [get_phone_call_information](../../../../../../../functions/crates/lpe-exchange/src/service/ews/unified_messaging/ExchangeService/get_phone_call_information.md)