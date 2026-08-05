---
type: Rust Function
title: log_nspi_dn_to_mid_debug
resource: crates/lpe-exchange/src/mapi/nspi/diagnostics.rs#L4-L29
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_dn_to_mid_response
---

# Signature

`pub(super) fn log_nspi_dn_to_mid_debug( principal: &AccountPrincipal, request_type: &str, request_id: &str, request: &[u8], values: &[String], matched: &NspiDnToMidMatch, )`

# Called by

- [nspi_dn_to_mid_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_dn_to_mid_response.md)