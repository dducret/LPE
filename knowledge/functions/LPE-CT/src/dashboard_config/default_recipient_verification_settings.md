---
type: Rust Function
title: default_recipient_verification_settings
resource: LPE-CT/src/dashboard_config.rs#L921-L927
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/dashboard_config/default_recipient_verification_cache_ttl_seconds
  called_by:
  - functions/LPE-CT/src/dashboard_config/default_state
---

# Signature

`pub(crate) fn default_recipient_verification_settings() -> RecipientVerificationSettings`

# Calls

- [default_recipient_verification_cache_ttl_seconds](../../../../functions/LPE-CT/src/dashboard_config/default_recipient_verification_cache_ttl_seconds.md)

# Called by

- [default_state](../../../../functions/LPE-CT/src/dashboard_config/default_state.md)