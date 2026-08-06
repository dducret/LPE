---
type: Rust Function
title: test_account_legacy_dn
resource: crates/lpe-exchange/src/tests/mapi_over_http.rs#L43-L45
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/test_legacy_dn
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_microsoft_oxocfg_same_target_wlinks_round_trip_distinctly
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/nspi_principal_mid
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_dn_to_mid_preserves_large_array_order_and_duplicates
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_dn_to_mid_rejects_truncated_auxiliary_buffer
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_dn_to_mid_rejects_trailing_bytes_after_auxiliary_buffer
---

# Signature

`fn test_account_legacy_dn(email: &str) -> String`

# Calls

- [test_legacy_dn](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/test_legacy_dn.md)

# Called by

- [mapi_over_http_microsoft_oxocfg_same_target_wlinks_round_trip_distinctly](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_microsoft_oxocfg_same_target_wlinks_round_trip_distinctly.md)
- [nspi_principal_mid](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/nspi_principal_mid.md)
- [mapi_over_http_nspi_dn_to_mid_preserves_large_array_order_and_duplicates](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_dn_to_mid_preserves_large_array_order_and_duplicates.md)
- [mapi_over_http_nspi_dn_to_mid_rejects_truncated_auxiliary_buffer](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_dn_to_mid_rejects_truncated_auxiliary_buffer.md)
- [mapi_over_http_nspi_dn_to_mid_rejects_trailing_bytes_after_auxiliary_buffer](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_dn_to_mid_rejects_trailing_bytes_after_auxiliary_buffer.md)