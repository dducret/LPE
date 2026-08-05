---
type: Rust Function
title: nspi_legacy_dn_from_cn
resource: crates/lpe-exchange/src/mapi/nspi.rs#L1122-L1124
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_entry_legacy_dn_with_prefix
  - functions/crates/lpe-exchange/src/mapi/nspi/push_principal_legacy_dn_alias
---

# Signature

`fn nspi_legacy_dn_from_cn(cn: &str) -> String`

# Called by

- [nspi_entry_legacy_dn_with_prefix](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_entry_legacy_dn_with_prefix.md)
- [push_principal_legacy_dn_alias](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/push_principal_legacy_dn_alias.md)