---
type: Rust Module
title: normalization
resource: crates/lpe-domain/src/normalization.rs#L1-L147
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/std-fmt
  - external/super
  member_of:
  - packages/crates/lpe-domain
---

# Contains

- [NormalizationError](../../../../classes/crates/lpe-domain/src/normalization/NormalizationError.md)
- [fmt](../../../../functions/crates/lpe-domain/src/normalization/NormalizationError/fmt-display/fmt.md)
- [normalize_domain_name](../../../../functions/crates/lpe-domain/src/normalization/normalize_domain_name.md)
- [normalize_mailbox_domain](../../../../functions/crates/lpe-domain/src/normalization/normalize_mailbox_domain.md)
- [normalize_email](../../../../functions/crates/lpe-domain/src/normalization/normalize_email.md)
- [normalize_mailbox_email](../../../../functions/crates/lpe-domain/src/normalization/normalize_mailbox_email.md)
- [normalize_trimmed_lowercase](../../../../functions/crates/lpe-domain/src/normalization/normalize_trimmed_lowercase.md)
- [normalize_login_name](../../../../functions/crates/lpe-domain/src/normalization/normalize_login_name.md)
- [normalize_calendar_email](../../../../functions/crates/lpe-domain/src/normalization/normalize_calendar_email.md)
- [normalize_calendar_participation_status](../../../../functions/crates/lpe-domain/src/normalization/normalize_calendar_participation_status.md)
- [normalize_smtp_lookup_value](../../../../functions/crates/lpe-domain/src/normalization/normalize_smtp_lookup_value.md)
- [mailbox_email_normalizes_idna_domain_and_unicode_local_part](../../../../functions/crates/lpe-domain/src/normalization/mailbox_email_normalizes_idna_domain_and_unicode_local_part.md)
- [login_name_uses_hint_for_unqualified_username](../../../../functions/crates/lpe-domain/src/normalization/login_name_uses_hint_for_unqualified_username.md)
- [calendar_email_strips_mailto_prefix](../../../../functions/crates/lpe-domain/src/normalization/calendar_email_strips_mailto_prefix.md)
- [smtp_lookup_strips_transport_prefixes](../../../../functions/crates/lpe-domain/src/normalization/smtp_lookup_strips_transport_prefixes.md)

# Imports

- `std::fmt`
- `super::*`

# Member of

- [lpe-domain](../../../../packages/crates/lpe-domain.md)