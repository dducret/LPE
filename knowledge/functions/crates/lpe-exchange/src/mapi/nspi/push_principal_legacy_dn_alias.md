---
type: Rust Function
title: push_principal_legacy_dn_alias
resource: crates/lpe-exchange/src/mapi/nspi.rs#L1173-L1183
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_legacy_cn_from_source
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_legacy_dn_from_cn
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/principal_legacy_dn_aliases
---

# Signature

`fn push_principal_legacy_dn_alias(aliases: &mut Vec<String>, source: &str)`

# Calls

- [nspi_legacy_cn_from_source](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_legacy_cn_from_source.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [nspi_legacy_dn_from_cn](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_legacy_dn_from_cn.md)

# Called by

- [principal_legacy_dn_aliases](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/principal_legacy_dn_aliases.md)