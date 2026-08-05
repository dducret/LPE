---
type: Rust Function
title: normalize_local_db_network_scope
resource: LPE-CT/src/dashboard_config.rs#L1032-L1037
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/dashboard_config/default_local_db_network_scope
  called_by:
  - functions/LPE-CT/src/dashboard_config/normalize_local_data_stores
---

# Signature

`fn normalize_local_db_network_scope(value: &str) -> String`

# Calls

- [default_local_db_network_scope](../../../../functions/LPE-CT/src/dashboard_config/default_local_db_network_scope.md)

# Called by

- [normalize_local_data_stores](../../../../functions/LPE-CT/src/dashboard_config/normalize_local_data_stores.md)