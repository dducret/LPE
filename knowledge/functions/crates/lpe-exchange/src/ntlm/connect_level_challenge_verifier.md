---
type: Rust Function
title: connect_level_challenge_verifier
resource: crates/lpe-exchange/src/ntlm.rs#L12-L19
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/ntlm/challenge_token
  called_by:
  - functions/crates/lpe-exchange/src/ntlm/connect_level_challenge_verifier_is_ntlm_type2
  - functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_context_ack_body
---

# Signature

`pub(crate) fn connect_level_challenge_verifier() -> NtlmVerifier`

# Calls

- [challenge_token](../../../../../functions/crates/lpe-exchange/src/ntlm/challenge_token.md)

# Called by

- [connect_level_challenge_verifier_is_ntlm_type2](../../../../../functions/crates/lpe-exchange/src/ntlm/connect_level_challenge_verifier_is_ntlm_type2.md)
- [rpc_proxy_dce_context_ack_body](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_context_ack_body.md)