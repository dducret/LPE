---
type: Rust Method
title: from_raw_query
resource: crates/lpe-activesync/src/types.rs#L35-L44
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/types/looks_like_plain_query
  - functions/crates/lpe-activesync/src/types/parse_plain_query
  - functions/crates/lpe-activesync/src/types/parse_base64_query
  called_by:
  - functions/crates/lpe-activesync/src/app/options_handler
  - functions/crates/lpe-activesync/src/app/post_handler
  - functions/crates/lpe-activesync/src/tests/parsed_base64_query
  - functions/crates/lpe-activesync/src/tests/base64_query_decodes_ashttp_fields
  - functions/crates/lpe-activesync/src/tests/plain_query_parsing_keeps_existing_fields
  - functions/crates/lpe-activesync/src/tests/malformed_base64_query_is_rejected_predictably
  - functions/crates/lpe-activesync/src/tests/base64_query_rejects_unsupported_protocol_version
---

# Signature

`pub(crate) fn from_raw_query(raw_query: Option<&str>) -> Result<Self>`

# Calls

- [looks_like_plain_query](../../../../../../functions/crates/lpe-activesync/src/types/looks_like_plain_query.md)
- [parse_plain_query](../../../../../../functions/crates/lpe-activesync/src/types/parse_plain_query.md)
- [parse_base64_query](../../../../../../functions/crates/lpe-activesync/src/types/parse_base64_query.md)

# Called by

- [options_handler](../../../../../../functions/crates/lpe-activesync/src/app/options_handler.md)
- [post_handler](../../../../../../functions/crates/lpe-activesync/src/app/post_handler.md)
- [parsed_base64_query](../../../../../../functions/crates/lpe-activesync/src/tests/parsed_base64_query.md)
- [base64_query_decodes_ashttp_fields](../../../../../../functions/crates/lpe-activesync/src/tests/base64_query_decodes_ashttp_fields.md)
- [plain_query_parsing_keeps_existing_fields](../../../../../../functions/crates/lpe-activesync/src/tests/plain_query_parsing_keeps_existing_fields.md)
- [malformed_base64_query_is_rejected_predictably](../../../../../../functions/crates/lpe-activesync/src/tests/malformed_base64_query_is_rejected_predictably.md)
- [base64_query_rejects_unsupported_protocol_version](../../../../../../functions/crates/lpe-activesync/src/tests/base64_query_rejects_unsupported_protocol_version.md)