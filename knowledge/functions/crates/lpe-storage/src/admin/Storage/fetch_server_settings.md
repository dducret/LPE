---
type: Rust Method
title: fetch_server_settings
resource: crates/lpe-storage/src/admin.rs#L1135-L1172
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/util/env_hostname
  - functions/crates/lpe-storage/src/util/env_bind_address
  called_by:
  - functions/crates/lpe-storage/src/admin/dashboard/Storage/fetch_admin_dashboard
---

# Signature

`async fn fetch_server_settings(&self) -> Result<ServerSettings>`

# Calls

- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [env_hostname](../../../../../../functions/crates/lpe-storage/src/util/env_hostname.md)
- [env_bind_address](../../../../../../functions/crates/lpe-storage/src/util/env_bind_address.md)

# Called by

- [fetch_admin_dashboard](../../../../../../functions/crates/lpe-storage/src/admin/dashboard/Storage/fetch_admin_dashboard.md)