---
type: Rust Function
title: normalize_trimmed_lowercase
resource: crates/lpe-domain/src/normalization.rs#L67-L69
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-domain/src/normalization/normalize_login_name
  - functions/crates/lpe-exchange/src/service/ews/delegation/parse_ews_delegate_user
  - functions/crates/lpe-exchange/src/service/ews/delegation/parse_delegate_user_id_emails
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_requested_smtp_address
  - functions/crates/lpe-jmap/src/contacts/parse_contact_email
---

# Signature

`pub fn normalize_trimmed_lowercase(value: &str) -> String`

# Called by

- [normalize_login_name](../../../../../functions/crates/lpe-domain/src/normalization/normalize_login_name.md)
- [parse_ews_delegate_user](../../../../../functions/crates/lpe-exchange/src/service/ews/delegation/parse_ews_delegate_user.md)
- [parse_delegate_user_id_emails](../../../../../functions/crates/lpe-exchange/src/service/ews/delegation/parse_delegate_user_id_emails.md)
- [rpc_proxy_nspi_requested_smtp_address](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_requested_smtp_address.md)
- [parse_contact_email](../../../../../functions/crates/lpe-jmap/src/contacts/parse_contact_email.md)