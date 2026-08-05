---
type: Rust Module
title: readiness
resource: crates/lpe-admin-api/src/readiness.rs#L1-L158
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/crate-types-readinesscheck-readinessresponse
  - external/std-path-pathbuf
  - external/std-time-duration
  member_of:
  - packages/crates/lpe-admin-api
---

# Contains

- [lpe_ct_base_url](../../../../functions/crates/lpe-admin-api/src/readiness/lpe_ct_base_url.md)
- [ha_role_file](../../../../functions/crates/lpe-admin-api/src/readiness/ha_role_file.md)
- [read_ha_role](../../../../functions/crates/lpe-admin-api/src/readiness/read_ha_role.md)
- [ha_activation_check](../../../../functions/crates/lpe-admin-api/src/readiness/ha_activation_check.md)
- [readiness_ok](../../../../functions/crates/lpe-admin-api/src/readiness/readiness_ok.md)
- [readiness_warn](../../../../functions/crates/lpe-admin-api/src/readiness/readiness_warn.md)
- [readiness_failed](../../../../functions/crates/lpe-admin-api/src/readiness/readiness_failed.md)
- [build_readiness_response](../../../../functions/crates/lpe-admin-api/src/readiness/build_readiness_response.md)
- [check_optional_http_dependency](../../../../functions/crates/lpe-admin-api/src/readiness/check_optional_http_dependency.md)
- [ha_allows_active_work](../../../../functions/crates/lpe-admin-api/src/readiness/ha_allows_active_work.md)
- [ha_current_role](../../../../functions/crates/lpe-admin-api/src/readiness/ha_current_role.md)

# Imports

- `crate::types::{ReadinessCheck, ReadinessResponse}`
- `std::path::PathBuf`
- `std::time::Duration`

# Member of

- [lpe-admin-api](../../../../packages/crates/lpe-admin-api.md)