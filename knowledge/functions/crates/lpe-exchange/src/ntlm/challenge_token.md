---
type: Rust Function
title: challenge_token
resource: crates/lpe-exchange/src/ntlm.rs#L21-L49
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/ntlm/connect_level_challenge_verifier
---

# Signature

`fn challenge_token() -> Vec<u8>`

# Called by

- [connect_level_challenge_verifier](../../../../../functions/crates/lpe-exchange/src/ntlm/connect_level_challenge_verifier.md)