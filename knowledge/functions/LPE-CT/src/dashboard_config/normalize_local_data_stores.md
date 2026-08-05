---
type: Rust Function
title: normalize_local_data_stores
resource: LPE-CT/src/dashboard_config.rs#L626-L659
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/dashboard_config/default_spool_queues
  - functions/LPE-CT/src/dashboard_config/default_policy_artifacts
  - functions/LPE-CT/src/dashboard_config/default_forbidden_canonical_data
  - functions/LPE-CT/src/dashboard_config/normalize_local_db_network_scope
  - functions/LPE-CT/src/dashboard_config/default_local_db_listen_address
  - functions/LPE-CT/src/dashboard_config/default_local_db_purposes
  called_by:
  - functions/LPE-CT/src/main
  - functions/LPE-CT/src/env_overrides_enable_private_local_db_profile
---

# Signature

`pub(crate) fn normalize_local_data_stores(local_data_stores: &mut LocalDataStoresSettings)`

# Calls

- [default_spool_queues](../../../../functions/LPE-CT/src/dashboard_config/default_spool_queues.md)
- [default_policy_artifacts](../../../../functions/LPE-CT/src/dashboard_config/default_policy_artifacts.md)
- [default_forbidden_canonical_data](../../../../functions/LPE-CT/src/dashboard_config/default_forbidden_canonical_data.md)
- [normalize_local_db_network_scope](../../../../functions/LPE-CT/src/dashboard_config/normalize_local_db_network_scope.md)
- [default_local_db_listen_address](../../../../functions/LPE-CT/src/dashboard_config/default_local_db_listen_address.md)
- [default_local_db_purposes](../../../../functions/LPE-CT/src/dashboard_config/default_local_db_purposes.md)

# Called by

- [main](../../../../functions/LPE-CT/src/main.md)
- [env_overrides_enable_private_local_db_profile](../../../../functions/LPE-CT/src/env_overrides_enable_private_local_db_profile.md)