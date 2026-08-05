---
type: Rust Function
title: stable_key_id
resource: LPE-CT/src/smtp.rs#L1177-L1181
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/smtp/anti_abuse/evaluate_greylisting
  - functions/LPE-CT/src/smtp/tests/greylisting_defers_first_triplet_then_allows_after_release_window
---

# Signature

`fn stable_key_id<T: Hash>(value: &T) -> String`

# Called by

- [evaluate_greylisting](../../../../functions/LPE-CT/src/smtp/anti_abuse/evaluate_greylisting.md)
- [greylisting_defers_first_triplet_then_allows_after_release_window](../../../../functions/LPE-CT/src/smtp/tests/greylisting_defers_first_triplet_then_allows_after_release_window.md)