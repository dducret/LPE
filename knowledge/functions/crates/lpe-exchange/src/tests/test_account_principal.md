---
type: Rust Function
title: test_account_principal
resource: crates/lpe-exchange/src/tests/mod.rs#L12610-L12620
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_emsmdb_logon_uses_authenticated_canonical_principal
  - functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_emsmdb_query_rows_reads_root_hierarchy_without_ipm_children
  - functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_emsmdb_rpc_ext2_parse_failure_returns_protocol_fault
  - functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_emsmdb_rpc_ext2_requires_authenticated_context
  - functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_address_book_check_name_fallback_answers_framing_mismatch
  - functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_address_book_auth3_does_not_trigger_check_name_fallback
  - functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_address_book_management_stats_accepts_rca_short_stub
  - functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_referral_get_fqdn_accepts_rca_short_stub
---

# Signature

`fn test_account_principal() -> AccountPrincipal`

# Called by

- [rpc_proxy_emsmdb_logon_uses_authenticated_canonical_principal](../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_emsmdb_logon_uses_authenticated_canonical_principal.md)
- [rpc_proxy_emsmdb_query_rows_reads_root_hierarchy_without_ipm_children](../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_emsmdb_query_rows_reads_root_hierarchy_without_ipm_children.md)
- [rpc_proxy_emsmdb_rpc_ext2_parse_failure_returns_protocol_fault](../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_emsmdb_rpc_ext2_parse_failure_returns_protocol_fault.md)
- [rpc_proxy_emsmdb_rpc_ext2_requires_authenticated_context](../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_emsmdb_rpc_ext2_requires_authenticated_context.md)
- [rpc_proxy_address_book_check_name_fallback_answers_framing_mismatch](../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_address_book_check_name_fallback_answers_framing_mismatch.md)
- [rpc_proxy_address_book_auth3_does_not_trigger_check_name_fallback](../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_address_book_auth3_does_not_trigger_check_name_fallback.md)
- [rpc_proxy_address_book_management_stats_accepts_rca_short_stub](../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_address_book_management_stats_accepts_rca_short_stub.md)
- [rpc_proxy_referral_get_fqdn_accepts_rca_short_stub](../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_referral_get_fqdn_accepts_rca_short_stub.md)