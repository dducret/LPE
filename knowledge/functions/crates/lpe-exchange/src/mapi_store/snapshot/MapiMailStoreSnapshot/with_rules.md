---
type: Rust Method
title: with_rules
resource: crates/lpe-exchange/src/mapi_store/snapshot.rs#L146-L163
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/tests/rule_table_projects_canonical_sieve_rule
  - functions/crates/lpe-exchange/src/mapi_store/T/mapistore/load_mapi_mail_store
---

# Signature

`pub(crate) fn with_rules(mut self, rules: Vec<MailboxRule>) -> Self`

# Called by

- [rule_table_projects_canonical_sieve_rule](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/rule_table_projects_canonical_sieve_rule.md)
- [load_mapi_mail_store](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/T/mapistore/load_mapi_mail_store.md)