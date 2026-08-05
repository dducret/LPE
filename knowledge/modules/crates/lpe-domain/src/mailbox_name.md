---
type: Rust Module
title: mailbox_name
resource: crates/lpe-domain/src/mailbox_name.rs#L1-L475
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/std-error-error-fmt
  - external/unicode-normalization-unicodenormalization
  - external/unicode-security-is-potential-mixed-script-confusable-char-skeleton-mixedscript
  member_of:
  - packages/crates/lpe-domain
---

# Contains

- [MailboxDisplayName](../../../../classes/crates/lpe-domain/src/mailbox_name/MailboxDisplayName.md)
- [MailboxSegment](../../../../classes/crates/lpe-domain/src/mailbox_name/MailboxSegment.md)
- [MailboxPath](../../../../classes/crates/lpe-domain/src/mailbox_name/MailboxPath.md)
- [MailboxCanonicalKey](../../../../classes/crates/lpe-domain/src/mailbox_name/MailboxCanonicalKey.md)
- [MailboxNameError](../../../../classes/crates/lpe-domain/src/mailbox_name/MailboxNameError.md)
- [new](../../../../functions/crates/lpe-domain/src/mailbox_name/MailboxDisplayName/new.md)
- [system](../../../../functions/crates/lpe-domain/src/mailbox_name/MailboxDisplayName/system.md)
- [as_str](../../../../functions/crates/lpe-domain/src/mailbox_name/MailboxDisplayName/as_str.md)
- [into_string](../../../../functions/crates/lpe-domain/src/mailbox_name/MailboxDisplayName/into_string.md)
- [canonical_key](../../../../functions/crates/lpe-domain/src/mailbox_name/MailboxDisplayName/canonical_key.md)
- [validate](../../../../functions/crates/lpe-domain/src/mailbox_name/MailboxDisplayName/validate.md)
- [new](../../../../functions/crates/lpe-domain/src/mailbox_name/MailboxSegment/new.md)
- [system](../../../../functions/crates/lpe-domain/src/mailbox_name/MailboxSegment/system.md)
- [as_str](../../../../functions/crates/lpe-domain/src/mailbox_name/MailboxSegment/as_str.md)
- [display_name](../../../../functions/crates/lpe-domain/src/mailbox_name/MailboxSegment/display_name.md)
- [into_display_name](../../../../functions/crates/lpe-domain/src/mailbox_name/MailboxSegment/into_display_name.md)
- [parse](../../../../functions/crates/lpe-domain/src/mailbox_name/MailboxPath/parse.md)
- [system](../../../../functions/crates/lpe-domain/src/mailbox_name/MailboxPath/system.md)
- [parse_with_reserved_policy](../../../../functions/crates/lpe-domain/src/mailbox_name/MailboxPath/parse_with_reserved_policy.md)
- [as_str](../../../../functions/crates/lpe-domain/src/mailbox_name/MailboxPath/as_str.md)
- [segments](../../../../functions/crates/lpe-domain/src/mailbox_name/MailboxPath/segments.md)
- [into_string](../../../../functions/crates/lpe-domain/src/mailbox_name/MailboxPath/into_string.md)
- [for_display_name](../../../../functions/crates/lpe-domain/src/mailbox_name/MailboxCanonicalKey/for_display_name.md)
- [as_str](../../../../functions/crates/lpe-domain/src/mailbox_name/MailboxCanonicalKey/as_str.md)
- [skeleton](../../../../functions/crates/lpe-domain/src/mailbox_name/MailboxCanonicalKey/skeleton.md)
- [collides_with](../../../../functions/crates/lpe-domain/src/mailbox_name/MailboxCanonicalKey/collides_with.md)
- [MailboxNamePolicy](../../../../classes/crates/lpe-domain/src/mailbox_name/MailboxNamePolicy.md)
- [canonical_key](../../../../functions/crates/lpe-domain/src/mailbox_name/MailboxNamePolicy/canonical_key.md)
- [list_pattern_matches](../../../../functions/crates/lpe-domain/src/mailbox_name/MailboxNamePolicy/list_pattern_matches.md)
- [system_role_for_display_name](../../../../functions/crates/lpe-domain/src/mailbox_name/MailboxNamePolicy/system_role_for_display_name.md)
- [canonical_system_display_name](../../../../functions/crates/lpe-domain/src/mailbox_name/MailboxNamePolicy/canonical_system_display_name.md)
- [is_reserved_key](../../../../functions/crates/lpe-domain/src/mailbox_name/MailboxNamePolicy/is_reserved_key.md)
- [fmt](../../../../functions/crates/lpe-domain/src/mailbox_name/MailboxNameError/fmt-display/fmt.md)
- [fmt](../../../../functions/crates/lpe-domain/src/mailbox_name/MailboxDisplayName/fmt-display/fmt.md)
- [fmt](../../../../functions/crates/lpe-domain/src/mailbox_name/MailboxSegment/fmt-display/fmt.md)
- [fmt](../../../../functions/crates/lpe-domain/src/mailbox_name/MailboxPath/fmt-display/fmt.md)
- [ReservedNamePolicy](../../../../classes/crates/lpe-domain/src/mailbox_name/ReservedNamePolicy.md)
- [ReservedMailboxName](../../../../classes/crates/lpe-domain/src/mailbox_name/ReservedMailboxName.md)
- [ReservedMailboxKey](../../../../classes/crates/lpe-domain/src/mailbox_name/ReservedMailboxKey.md)
- [matches](../../../../functions/crates/lpe-domain/src/mailbox_name/ReservedMailboxKey/matches.md)
- [reserved](../../../../functions/crates/lpe-domain/src/mailbox_name/reserved.md)
- [validate_raw_segment](../../../../functions/crates/lpe-domain/src/mailbox_name/validate_raw_segment.md)
- [validate_normalized_segment](../../../../functions/crates/lpe-domain/src/mailbox_name/validate_normalized_segment.md)
- [validate_codepoints](../../../../functions/crates/lpe-domain/src/mailbox_name/validate_codepoints.md)
- [has_ascii_boundary_whitespace](../../../../functions/crates/lpe-domain/src/mailbox_name/has_ascii_boundary_whitespace.md)
- [is_private_use](../../../../functions/crates/lpe-domain/src/mailbox_name/is_private_use.md)
- [is_unsafe_invisible](../../../../functions/crates/lpe-domain/src/mailbox_name/is_unsafe_invisible.md)
- [has_mixed_script_confusable](../../../../functions/crates/lpe-domain/src/mailbox_name/has_mixed_script_confusable.md)
- [confusable_skeleton](../../../../functions/crates/lpe-domain/src/mailbox_name/confusable_skeleton.md)
- [fold_for_comparison](../../../../functions/crates/lpe-domain/src/mailbox_name/fold_for_comparison.md)
- [fold_list_pattern_text](../../../../functions/crates/lpe-domain/src/mailbox_name/fold_list_pattern_text.md)
- [list_pattern_match_from](../../../../functions/crates/lpe-domain/src/mailbox_name/list_pattern_match_from.md)

# Imports

- `std::{error::Error, fmt}`
- `unicode_normalization::UnicodeNormalization`
- `unicode_security::{is_potential_mixed_script_confusable_char, skeleton, MixedScript}`

# Member of

- [lpe-domain](../../../../packages/crates/lpe-domain.md)