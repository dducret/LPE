---
type: Python Function
title: check_mapi_gate1_readiness
resource: tools/rca_outlook_connectivity_check.py#L181-L342
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/rca_outlook/http/require
  - functions/tools/rca_outlook/http/join_url
  - functions/tools/rca_outlook/http/content_type
  - functions/tools/rca_outlook/mapi/parse_pox_mapi_http_endpoints
  - functions/tools/rca_outlook/mapi/require_published_mapi_url
  - functions/tools/rca_outlook_connectivity_check/gate1_diagnostic
  - functions/tools/rca_outlook/http/basic_auth_header
  - functions/tools/rca_outlook/http/require_guid_counter_header
  - functions/tools/rca_outlook/mapi/mapi_session_cookie_state
  - functions/tools/rca_outlook/http/cookie_header
  - functions/tools/rca_outlook_connectivity_check/execute
  - functions/tools/rca_outlook_connectivity_check/mapi_gate1_bootstrap_rops
  - functions/tools/rca_outlook_connectivity_check/mapi_gate1_hierarchy_rows
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/CounterSet/intersection
  called_by:
  - functions/tools/rca_outlook_connectivity_check/main
  - functions/tools/test_rca_outlook_connectivity_check/MapiGate1ReadinessTests/test_gate1_uses_discovered_urls_and_carries_emsmdb_cookies
---

# Signature

`def check_mapi_gate1_readiness( base_url: str, email: str, password: str, expected_service_host: str, insecure_tls: bool, timeout: int, ) -> None:`

# Calls

- [require](../../../functions/tools/rca_outlook/http/require.md)
- [join_url](../../../functions/tools/rca_outlook/http/join_url.md)
- [content_type](../../../functions/tools/rca_outlook/http/content_type.md)
- [parse_pox_mapi_http_endpoints](../../../functions/tools/rca_outlook/mapi/parse_pox_mapi_http_endpoints.md)
- [require_published_mapi_url](../../../functions/tools/rca_outlook/mapi/require_published_mapi_url.md)
- [gate1_diagnostic](../../../functions/tools/rca_outlook_connectivity_check/gate1_diagnostic.md)
- [basic_auth_header](../../../functions/tools/rca_outlook/http/basic_auth_header.md)
- [require_guid_counter_header](../../../functions/tools/rca_outlook/http/require_guid_counter_header.md)
- [mapi_session_cookie_state](../../../functions/tools/rca_outlook/mapi/mapi_session_cookie_state.md)
- [cookie_header](../../../functions/tools/rca_outlook/http/cookie_header.md)
- [execute](../../../functions/tools/rca_outlook_connectivity_check/execute.md)
- [mapi_gate1_bootstrap_rops](../../../functions/tools/rca_outlook_connectivity_check/mapi_gate1_bootstrap_rops.md)
- [mapi_gate1_hierarchy_rows](../../../functions/tools/rca_outlook_connectivity_check/mapi_gate1_hierarchy_rows.md)
- [intersection](../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/CounterSet/intersection.md)

# Called by

- [main](../../../functions/tools/rca_outlook_connectivity_check/main.md)
- [test_gate1_uses_discovered_urls_and_carries_emsmdb_cookies](../../../functions/tools/test_rca_outlook_connectivity_check/MapiGate1ReadinessTests/test_gate1_uses_discovered_urls_and_carries_emsmdb_cookies.md)