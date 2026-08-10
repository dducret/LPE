---
type: Rust Function
title: greylisting_defers_first_triplet_then_allows_after_release_window
resource: LPE-CT/src/smtp/tests.rs#L2448-L2475
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/smtp/initialize_spool
  - functions/LPE-CT/src/smtp/tests/runtime_config
  - functions/LPE-CT/src/smtp/anti_abuse/evaluate_greylisting
  - functions/LPE-CT/src/smtp/stable_key_id
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
---

# Signature

`async fn greylisting_defers_first_triplet_then_allows_after_release_window()`

# Calls

- [initialize_spool](../../../../../functions/LPE-CT/src/smtp/initialize_spool.md)
- [runtime_config](../../../../../functions/LPE-CT/src/smtp/tests/runtime_config.md)
- [evaluate_greylisting](../../../../../functions/LPE-CT/src/smtp/anti_abuse/evaluate_greylisting.md)
- [stable_key_id](../../../../../functions/LPE-CT/src/smtp/stable_key_id.md)
- [from_str](../../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)