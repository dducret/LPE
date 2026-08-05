---
type: Rust Function
title: sync_dashboard_configuration
resource: LPE-CT/src/storage.rs#L467-L1043
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-activesync/src/tests/query
  - functions/LPE-CT/src/storage/delete_stale_policy_address_rules
  - functions/LPE-CT/src/storage/delete_stale_attachment_policy_rules
  - functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from
  called_by:
  - functions/LPE-CT/src/sync_dashboard_to_postgres
---

# Signature

`pub(crate) async fn sync_dashboard_configuration( config: &LocalDbConfig, dashboard: &crate::DashboardState, ) -> Result<()>`

# Calls

- [push](../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [query](../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [delete_stale_policy_address_rules](../../../../functions/LPE-CT/src/storage/delete_stale_policy_address_rules.md)
- [delete_stale_attachment_policy_rules](../../../../functions/LPE-CT/src/storage/delete_stale_attachment_policy_rules.md)
- [try_from](../../../../functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from.md)

# Called by

- [sync_dashboard_to_postgres](../../../../functions/LPE-CT/src/sync_dashboard_to_postgres.md)