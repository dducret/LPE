---
type: Rust Module
title: values
resource: crates/lpe-jmap/src/mail/values.rs#L1-L663
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-bail-result
  - external/base64-engine-general-purpose-standard-as-base64-engine-as
  - external/lpe-storage-jmapemail-jmapemailsubmission-jmapquota-senderidentity
  - external/serde-json-json-map-value
  - external/std-cmp-ordering-collections-hashset
  - external/uuid-uuid
  - external/crate-convert-address-value-insert-if-protocol-emailgetarguments-emailqueryfilter-emailquerysort-emailsubmissionqueryfilter-emailsubmissionquerysort
  member_of:
  - packages/crates/lpe-jmap
---

# Contains

- [full_query_limit](../../../../../functions/crates/lpe-jmap/src/mail/values/full_query_limit.md)
- [serialize_email_query_filter](../../../../../functions/crates/lpe-jmap/src/mail/values/serialize_email_query_filter.md)
- [serialize_email_query_sort](../../../../../functions/crates/lpe-jmap/src/mail/values/serialize_email_query_sort.md)
- [validate_email_submission_query](../../../../../functions/crates/lpe-jmap/src/mail/values/validate_email_submission_query.md)
- [apply_email_submission_query](../../../../../functions/crates/lpe-jmap/src/mail/values/apply_email_submission_query.md)
- [email_submission_matches_filter](../../../../../functions/crates/lpe-jmap/src/mail/values/email_submission_matches_filter.md)
- [compare_email_submission_sort_key](../../../../../functions/crates/lpe-jmap/src/mail/values/compare_email_submission_sort_key.md)
- [serialize_email_submission_query_sort](../../../../../functions/crates/lpe-jmap/src/mail/values/serialize_email_submission_query_sort.md)
- [email_properties](../../../../../functions/crates/lpe-jmap/src/mail/values/email_properties.md)
- [email_submission_properties](../../../../../functions/crates/lpe-jmap/src/mail/values/email_submission_properties.md)
- [identity_properties](../../../../../functions/crates/lpe-jmap/src/mail/values/identity_properties.md)
- [thread_properties](../../../../../functions/crates/lpe-jmap/src/mail/values/thread_properties.md)
- [EmailBodyOptions](../../../../../classes/crates/lpe-jmap/src/mail/values/EmailBodyOptions.md)
- [from_arguments](../../../../../functions/crates/lpe-jmap/src/mail/values/EmailBodyOptions/from_arguments.md)
- [should_fetch_text_value](../../../../../functions/crates/lpe-jmap/src/mail/values/EmailBodyOptions/should_fetch_text_value.md)
- [should_fetch_html_value](../../../../../functions/crates/lpe-jmap/src/mail/values/EmailBodyOptions/should_fetch_html_value.md)
- [email_to_value](../../../../../functions/crates/lpe-jmap/src/mail/values/email_to_value.md)
- [body_part_value](../../../../../functions/crates/lpe-jmap/src/mail/values/body_part_value.md)
- [body_value](../../../../../functions/crates/lpe-jmap/src/mail/values/body_value.md)
- [email_submission_to_value](../../../../../functions/crates/lpe-jmap/src/mail/values/email_submission_to_value.md)
- [identity_to_value](../../../../../functions/crates/lpe-jmap/src/mail/values/identity_to_value.md)
- [thread_to_value](../../../../../functions/crates/lpe-jmap/src/mail/values/thread_to_value.md)
- [search_snippet_to_value](../../../../../functions/crates/lpe-jmap/src/mail/values/search_snippet_to_value.md)
- [quota_to_value](../../../../../functions/crates/lpe-jmap/src/mail/values/quota_to_value.md)
- [email_keywords](../../../../../functions/crates/lpe-jmap/src/mail/values/email_keywords.md)
- [email_followup_value](../../../../../functions/crates/lpe-jmap/src/mail/values/email_followup_value.md)

# Imports

- `anyhow::{bail, Result}`
- `base64::{engine::general_purpose::STANDARD as BASE64, Engine as _}`
- `lpe_storage::{JmapEmail, JmapEmailSubmission, JmapQuota, SenderIdentity}`
- `serde_json::{json, Map, Value}`
- `std::{cmp::Ordering, collections::HashSet}`
- `uuid::Uuid`
- `crate::{
    convert::{address_value, insert_if},
    protocol::{
        EmailGetArguments, EmailQueryFilter, EmailQuerySort, EmailSubmissionQueryFilter,
        EmailSubmissionQuerySort,
    },
}`

# Member of

- [lpe-jmap](../../../../../packages/crates/lpe-jmap.md)