---
type: Rust Module
title: ids
resource: crates/lpe-exchange/src/service/ews/ids.rs#L1-L290
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/base64-engine-general-purpose-url-safe-no-pad
  - external/sha2-digest-sha256
  - external/super-super
  member_of:
  - packages/crates/lpe-exchange
---

# Contains

- [ConvertIdSource](../../../../../../classes/crates/lpe-exchange/src/service/ews/ids/ConvertIdSource.md)
- [CanonicalEwsObjectId](../../../../../../classes/crates/lpe-exchange/src/service/ews/ids/CanonicalEwsObjectId.md)
- [ConvertIdOutput](../../../../../../classes/crates/lpe-exchange/src/service/ews/ids/ConvertIdOutput.md)
- [convert_id_success_response](../../../../../../functions/crates/lpe-exchange/src/service/ews/ids/convert_id_success_response.md)
- [convert_id_xml](../../../../../../functions/crates/lpe-exchange/src/service/ews/ids/convert_id_xml.md)
- [canonical_message_id_from_ews_id](../../../../../../functions/crates/lpe-exchange/src/service/ews/ids/canonical_message_id_from_ews_id.md)
- [versioned_change_key](../../../../../../functions/crates/lpe-exchange/src/service/ews/ids/versioned_change_key.md)
- [requested_convert_ids](../../../../../../functions/crates/lpe-exchange/src/service/ews/ids/requested_convert_ids.md)
- [convert_id_sources_for_tag](../../../../../../functions/crates/lpe-exchange/src/service/ews/ids/convert_id_sources_for_tag.md)
- [canonical_ews_object_id_from_convert_source](../../../../../../functions/crates/lpe-exchange/src/service/ews/ids/canonical_ews_object_id_from_convert_source.md)
- [canonical_ews_object_id_from_payload](../../../../../../functions/crates/lpe-exchange/src/service/ews/ids/canonical_ews_object_id_from_payload.md)
- [canonical_ews_object_id_from_canonical_id](../../../../../../functions/crates/lpe-exchange/src/service/ews/ids/canonical_ews_object_id_from_canonical_id.md)
- [canonical_ews_family](../../../../../../functions/crates/lpe-exchange/src/service/ews/ids/canonical_ews_family.md)
- [convert_canonical_ews_object_id](../../../../../../functions/crates/lpe-exchange/src/service/ews/ids/convert_canonical_ews_object_id.md)
- [normalize_convert_id_format](../../../../../../functions/crates/lpe-exchange/src/service/ews/ids/normalize_convert_id_format.md)
- [opaque_ews_id](../../../../../../functions/crates/lpe-exchange/src/service/ews/ids/opaque_ews_id.md)
- [encode_hex_entry_id](../../../../../../functions/crates/lpe-exchange/src/service/ews/ids/encode_hex_entry_id.md)
- [decode_hex_entry_id](../../../../../../functions/crates/lpe-exchange/src/service/ews/ids/decode_hex_entry_id.md)
- [convert_id](../../../../../../functions/crates/lpe-exchange/src/service/ews/ids/ExchangeService/convert_id.md)

# Imports

- `base64::engine::general_purpose::URL_SAFE_NO_PAD`
- `sha2::{Digest, Sha256}`
- `super::super::*`

# Member of

- [lpe-exchange](../../../../../../packages/crates/lpe-exchange.md)