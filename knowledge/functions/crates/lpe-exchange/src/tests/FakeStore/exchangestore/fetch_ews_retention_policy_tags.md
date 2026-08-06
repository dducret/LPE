---
type: Rust Method
title: fetch_ews_retention_policy_tags
resource: crates/lpe-exchange/src/tests/mod.rs#L5155-L5175
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/retention/ExchangeService/get_user_retention_policy_tags
---

# Signature

`fn fetch_ews_retention_policy_tags<'a>( &'a self, principal: &'a AccountPrincipal, ) -> StoreFuture<'a, Vec<EwsRetentionPolicyTag>>`

# Called by

- [get_user_retention_policy_tags](../../../../../../../functions/crates/lpe-exchange/src/service/ews/retention/ExchangeService/get_user_retention_policy_tags.md)