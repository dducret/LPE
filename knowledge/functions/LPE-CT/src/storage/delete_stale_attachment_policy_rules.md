---
type: Rust Function
title: delete_stale_attachment_policy_rules
resource: LPE-CT/src/storage.rs#L1069-L1091
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  called_by:
  - functions/LPE-CT/src/storage/sync_dashboard_configuration
---

# Signature

`async fn delete_stale_attachment_policy_rules( tx: &mut sqlx::Transaction<'_, sqlx::Postgres>, rule_scope: &str, action: &str, active_values: &[String], ) -> Result<()>`

# Calls

- [query](../../../../functions/crates/lpe-activesync/src/tests/query.md)

# Called by

- [sync_dashboard_configuration](../../../../functions/LPE-CT/src/storage/sync_dashboard_configuration.md)