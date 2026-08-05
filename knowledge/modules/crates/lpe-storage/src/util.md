---
type: Rust Module
title: util
resource: crates/lpe-storage/src/util.rs#L1-L251
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-anyhow-bail-result
  - external/lpe-domain-normalization
  - external/lpe-domain-mailboxnamepolicy
  - external/std-env
  - external/uuid-uuid
  - external/pub-crate-use-lpe-domain-crypto-sha256-hex
  member_of:
  - packages/crates/lpe-storage
---

# Contains

- [normalize_mailbox_domain](../../../../functions/crates/lpe-storage/src/util/normalize_mailbox_domain.md)
- [normalize_mailbox_email](../../../../functions/crates/lpe-storage/src/util/normalize_mailbox_email.md)
- [normalize_email](../../../../functions/crates/lpe-storage/src/util/normalize_email.md)
- [normalize_domain_name](../../../../functions/crates/lpe-storage/src/util/normalize_domain_name.md)
- [normalize_admin_session_auth_method](../../../../functions/crates/lpe-storage/src/util/normalize_admin_session_auth_method.md)
- [normalize_subject](../../../../functions/crates/lpe-storage/src/util/normalize_subject.md)
- [normalize_task_status](../../../../functions/crates/lpe-storage/src/util/normalize_task_status.md)
- [normalize_task_list_name](../../../../functions/crates/lpe-storage/src/util/normalize_task_list_name.md)
- [domain_from_email](../../../../functions/crates/lpe-storage/src/util/domain_from_email.md)
- [preview_text](../../../../functions/crates/lpe-storage/src/util/preview_text.md)
- [permissions_from_storage](../../../../functions/crates/lpe-storage/src/util/permissions_from_storage.md)
- [system_mailbox_role_for_display_name](../../../../functions/crates/lpe-storage/src/util/system_mailbox_role_for_display_name.md)
- [canonical_system_mailbox_display_name](../../../../functions/crates/lpe-storage/src/util/canonical_system_mailbox_display_name.md)
- [normalize_admin_permissions](../../../../functions/crates/lpe-storage/src/util/normalize_admin_permissions.md)
- [permission_summary](../../../../functions/crates/lpe-storage/src/util/permission_summary.md)
- [default_permissions_for_role](../../../../functions/crates/lpe-storage/src/util/default_permissions_for_role.md)
- [parse_activesync_file_reference](../../../../functions/crates/lpe-storage/src/util/parse_activesync_file_reference.md)
- [trim_optional_text](../../../../functions/crates/lpe-storage/src/util/trim_optional_text.md)
- [normalize_gal_visibility](../../../../functions/crates/lpe-storage/src/util/normalize_gal_visibility.md)
- [normalize_directory_kind](../../../../functions/crates/lpe-storage/src/util/normalize_directory_kind.md)
- [validate_sieve_script_name](../../../../functions/crates/lpe-storage/src/util/validate_sieve_script_name.md)
- [validate_sieve_script_content](../../../../functions/crates/lpe-storage/src/util/validate_sieve_script_content.md)
- [env_hostname](../../../../functions/crates/lpe-storage/src/util/env_hostname.md)
- [env_bind_address](../../../../functions/crates/lpe-storage/src/util/env_bind_address.md)
- [split_permissions](../../../../functions/crates/lpe-storage/src/util/split_permissions.md)

# Imports

- `anyhow::{anyhow, bail, Result}`
- `lpe_domain::normalization`
- `lpe_domain::MailboxNamePolicy`
- `std::env`
- `uuid::Uuid`
- `pub(crate) use lpe_domain::crypto::sha256_hex`

# Member of

- [lpe-storage](../../../../packages/crates/lpe-storage.md)