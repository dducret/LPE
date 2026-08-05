---
type: Rust Function
title: get_user_retention_policy_tags_response
resource: crates/lpe-exchange/src/service/ews/retention.rs#L20-L36
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/retention/ExchangeService/get_user_retention_policy_tags
---

# Signature

`pub(in crate::service) fn get_user_retention_policy_tags_response( tags: &[EwsRetentionPolicyTag], ) -> String`

# Called by

- [get_user_retention_policy_tags](../../../../../../../functions/crates/lpe-exchange/src/service/ews/retention/ExchangeService/get_user_retention_policy_tags.md)