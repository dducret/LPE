---
type: Rust Function
title: contact_urls_json_from_mapi
resource: crates/lpe-exchange/src/mapi/properties/contact.rs#L586-L595
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/contact/upsert_labeled_value
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/contact/contact_input_from_mapi
---

# Signature

`fn contact_urls_json_from_mapi( existing: &serde_json::Value, personal: Option<&str>, business: Option<&str>, ) -> serde_json::Value`

# Calls

- [upsert_labeled_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/contact/upsert_labeled_value.md)

# Called by

- [contact_input_from_mapi](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/contact/contact_input_from_mapi.md)