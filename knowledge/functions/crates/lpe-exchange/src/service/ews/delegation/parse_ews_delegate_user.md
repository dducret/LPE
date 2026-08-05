---
type: Rust Function
title: parse_ews_delegate_user
resource: crates/lpe-exchange/src/service/ews/delegation.rs#L351-L388
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/element_content
  - functions/crates/lpe-exchange/src/service/ews/xml/element_text
  - functions/crates/lpe-domain/src/normalization/normalize_trimmed_lowercase
  - functions/crates/lpe-exchange/src/service/ews/delegation/reject_unsupported_delegate_permissions
  - functions/crates/lpe-exchange/src/service/ews/delegation/parse_delegate_permission_level
  - functions/crates/lpe-exchange/src/service/ews/xml/parse_xml_bool
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/delegation/parse_ews_delegate_users
---

# Signature

`fn parse_ews_delegate_user( principal: &AccountPrincipal, user: &str, delivery: &EwsDelegatePreferences, ) -> Result<UpsertEwsDelegateInput>`

# Calls

- [element_content](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_content.md)
- [element_text](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_text.md)
- [normalize_trimmed_lowercase](../../../../../../../functions/crates/lpe-domain/src/normalization/normalize_trimmed_lowercase.md)
- [reject_unsupported_delegate_permissions](../../../../../../../functions/crates/lpe-exchange/src/service/ews/delegation/reject_unsupported_delegate_permissions.md)
- [parse_delegate_permission_level](../../../../../../../functions/crates/lpe-exchange/src/service/ews/delegation/parse_delegate_permission_level.md)
- [parse_xml_bool](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/parse_xml_bool.md)

# Called by

- [parse_ews_delegate_users](../../../../../../../functions/crates/lpe-exchange/src/service/ews/delegation/parse_ews_delegate_users.md)