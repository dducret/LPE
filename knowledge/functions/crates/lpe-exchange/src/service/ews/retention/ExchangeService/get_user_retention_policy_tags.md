---
type: Rust Method
title: get_user_retention_policy_tags
resource: crates/lpe-exchange/src/service/ews/retention.rs#L8-L17
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_ews_retention_policy_tags
  - functions/crates/lpe-exchange/src/service/ews/retention/get_user_retention_policy_tags_response
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle
---

# Signature

`pub(in crate::service) async fn get_user_retention_policy_tags( &self, principal: &AccountPrincipal, ) -> Result<String>`

# Calls

- [fetch_ews_retention_policy_tags](../../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_ews_retention_policy_tags.md)
- [get_user_retention_policy_tags_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/retention/get_user_retention_policy_tags_response.md)

# Called by

- [handle](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle.md)