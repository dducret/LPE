---
type: Rust Method
title: into_text
resource: crates/lpe-exchange/src/mapi/properties/values.rs#L521-L542
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/rules/bounded_rule_mutation_from_row
  - functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_email_with_attachments
  - functions/crates/lpe-exchange/src/mapi/properties/restriction_matches
  - functions/crates/lpe-exchange/src/mapi/properties/message/apply_canonical_message_property_values
  - functions/crates/lpe-exchange/src/mapi/properties/message/message_followup_update_from_mapi_values
  - functions/crates/lpe-exchange/src/mapi/properties/streams/pending_text_property
  - functions/crates/lpe-exchange/src/mapi/properties/streams/optional_pending_text_property
  - functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value
  - functions/crates/lpe-exchange/src/mapi/rop/recipients/optional_mapi_value_text
  - functions/crates/lpe-exchange/src/mapi/rop/restrictions/parse_mapi_restriction_from
---

# Signature

`pub(in crate::mapi) fn into_text(self) -> Option<String>`

# Called by

- [bounded_rule_mutation_from_row](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/rules/bounded_rule_mutation_from_row.md)
- [restriction_matches_email_with_attachments](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_email_with_attachments.md)
- [restriction_matches](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches.md)
- [apply_canonical_message_property_values](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/apply_canonical_message_property_values.md)
- [message_followup_update_from_mapi_values](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/message_followup_update_from_mapi_values.md)
- [pending_text_property](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/pending_text_property.md)
- [optional_pending_text_property](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/optional_pending_text_property.md)
- [write_mapi_value](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value.md)
- [optional_mapi_value_text](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/recipients/optional_mapi_value_text.md)
- [parse_mapi_restriction_from](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/restrictions/parse_mapi_restriction_from.md)