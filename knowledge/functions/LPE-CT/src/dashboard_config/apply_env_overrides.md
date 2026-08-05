---
type: Rust Function
title: apply_env_overrides
resource: LPE-CT/src/dashboard_config.rs#L3-L217
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/dashboard_config/upsert_env_public_tls_profile
  - functions/LPE-CT/src/dashboard_config/parse_csv
  - functions/crates/lpe-core/src/sieve/Parser/next
  - functions/LPE-CT/src/dashboard_config/normalize_public_tls_settings
  called_by:
  - functions/LPE-CT/src/main
  - functions/LPE-CT/src/env_overrides_enable_private_local_db_profile
---

# Signature

`pub(crate) fn apply_env_overrides(state: &mut DashboardState)`

# Calls

- [upsert_env_public_tls_profile](../../../../functions/LPE-CT/src/dashboard_config/upsert_env_public_tls_profile.md)
- [parse_csv](../../../../functions/LPE-CT/src/dashboard_config/parse_csv.md)
- [next](../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)
- [normalize_public_tls_settings](../../../../functions/LPE-CT/src/dashboard_config/normalize_public_tls_settings.md)

# Called by

- [main](../../../../functions/LPE-CT/src/main.md)
- [env_overrides_enable_private_local_db_profile](../../../../functions/LPE-CT/src/env_overrides_enable_private_local_db_profile.md)