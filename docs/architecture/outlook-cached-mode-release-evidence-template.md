# Outlook Cached-Mode Release Evidence Template

## Current State/Functionality Overview

This template records Outlook 2016 and Outlook 2019 cached-mode `MAPI over HTTP`
release evidence. It is an evidence capture artifact only and does not change
runtime configuration. In 0.5.x, `LPE_AUTOCONFIG_MAPI_INTEROP_GATE_PASSED`
is the MAPI/HTTP publication gate; `LPE_AUTOCONFIG_OUTLOOK_INTEROP_GATE_PASSED`
remains reserved for legacy `EXPR`/RPC over HTTP.

Create one completed copy per deployment class and test window. Do not merge
Outlook 2016 and Outlook 2019 evidence; each client version needs its own real
profile result.

## Implementation/Usage

Complete the fields below for each release acceptance pass. Redact passwords,
tokens, session cookies, private message bodies, and unrelated mailbox content.
Keep enough trace identifiers, timestamps, and sanitized payload excerpts to
reproduce the result.

### Deployment and Account

| Field | Value |
| --- | --- |
| Evidence date | `<YYYY-MM-DD>` |
| Evidence owner | `<name or team>` |
| LPE commit/build | `<git sha, build id, package version>` |
| Deployment class | `<single-node sticky-session lab, staging, production-like>` |
| Public host | `<mail.example.test>` |
| Tenant | `<tenant/domain>` |
| Account | `<mailbox address>` |
| Auth method | `<Basic, bearer, other>` |
| TLS certificate | `<issuer, subject/SAN, expiry, validation status>` |
| Endpoint flags | `LPE_AUTOCONFIG_EWS_ENABLED=<true/false>; LPE_AUTOCONFIG_MAPI_ENABLED=<true/false>; LPE_AUTOCONFIG_MAPI_INTEROP_GATE_PASSED=<true/false>; LPE_AUTOCONFIG_EXCH_INTEROP_GATE_PASSED=<true/false>; LPE_AUTOCONFIG_EXPR_AUTODISCOVER_ENABLED=<true/false>` |

### Client Matrix

| Client | Outlook version/build | Windows build | Profile type | Cached mode | Result | Evidence |
| --- | --- | --- | --- | --- | --- | --- |
| Outlook 2016 | `<version/build/channel>` | `<Windows version/build>` | `<clean Exchange profile>` | `<enabled>` | `<pass/fail/not run>` | `<logs/screenshots/trace ids>` |
| Outlook 2019 | `<version/build/channel>` | `<Windows version/build>` | `<clean Exchange profile>` | `<enabled>` | `<pass/fail/not run>` | `<logs/screenshots/trace ids>` |

### Release Evidence Results

| Gate | Required evidence | Result | Evidence reference |
| --- | --- | --- | --- |
| MAPI/HTTP Gate 1 harness | `tools/rca_outlook_connectivity_check.py --mapi-gate1-readiness` against the public `LPE-CT` HTTPS host and a disposable mailbox: POX `mapiHttp`, EMSMDB Connect/private logon/root-IPM hierarchy, NSPI Bind | `<pass/fail/not run>` | `<command output, sanitized Autodiscover response, endpoints, request ids, hierarchy rows>` |
| Local harness | `cargo test -p lpe-exchange` and `tools/rca_outlook_connectivity_check.py --outlook-rca-readiness` against the target deployment shape | `<pass/fail/not run>` | `<command output, CI run, trace ids>` |
| Microsoft RCA | Microsoft Remote Connectivity Analyzer Outlook Connectivity against the same public host, account, tenant, TLS certificate, and endpoint flags | `<pass/fail/not run>` | `<RCA timestamp, test name, correlation id, exported report>` |
| Outlook 2016 real profile | Clean Outlook 2016 Exchange profile creates, syncs cached mode, reopens twice, resolves NSPI, submits via canonical LPE submission, and shows canonical `Sent` | `<pass/fail/not run>` | `<client logs, screenshots, server trace ids>` |
| Outlook 2019 real profile | Clean Outlook 2019 Exchange profile creates, syncs cached mode, reopens twice, resolves NSPI, submits via canonical LPE submission, and shows canonical `Sent` | `<pass/fail/not run>` | `<client logs, screenshots, server trace ids>` |

Record every row independently. Missing evidence is a release-quality risk and
must not be silently reported as a pass. It does not toggle MAPI publication.

### Core MAPI Acceptance Ledger

Record the same deployment revision and disposable mailbox for each row. These
are release-evidence checks, not a reason to publish RPC/HTTP or change
Autodiscover flags.

| Required flow | Local golden test / harness evidence | Microsoft RCA result | Outlook 2016 result | Outlook 2019 result | Evidence reference |
| --- | --- | --- | --- | --- | --- |
| Connect, private logon, Execute, reconnect, Disconnect, idle Ping | `mapi_over_http_microsoft_oxcmapihttp_connect_execute_reconnect_disconnect_sequence`; `mapi_over_http_microsoft_oxcmapihttp_ping_refreshes_idle_session_context` ([MS-OXCMAPIHTTP] sections 2.2.3.3.1 and 2.2.4) | `<pass/fail/not run>` | `<pass/fail/not run>` | `<pass/fail/not run>` | `<artifact>` |
| Cached hierarchy and contents synchronization after reopen | `mapi_over_http_hierarchy_sync_client_state_resumes_after_completed_download`; `calendar_sync_object_projects_stable_identity_and_attachment_presence`; `mapi_over_http_microsoft_oxcfxics_4_5_content_sync_stream_shape` ([MS-OXCFXICS] sections 2.2.4 and 3.1.5.3) | `<pass/fail/not run>` | `<pass/fail/not run>` | `<pass/fail/not run>` | `<artifact>` |
| Special folders, views, and configuration FAI | `mapi_over_http_inbox_additional_ren_entry_ids_versions_and_replays_hierarchy_in_postgresql`; `mapi_over_http_microsoft_oxocfg_configuration_examples_round_trip_fai` ([MS-OXOSFLD] sections 2.2.3-2.2.4; [MS-OXOCFG] sections 4.1-4.4) | `<pass/fail/not run>` | `<pass/fail/not run>` | `<pass/fail/not run>` | `<artifact>` |
| Canonical submission and authoritative `Sent` | `mapi_over_http_mail_lifecycle_uses_canonical_state_end_to_end` ([MS-OXCROPS] section 2.2) | `<pass/fail/not run>` | `<pass/fail/not run>` | `<pass/fail/not run>` | `<artifact>` |
| Permissions, notifications, and unsupported/malformed no-write paths | `mapi_over_http_shared_calendar_without_share_right_rejects_modify_permissions`; `mapi_over_http_notification_wait_reports_content_event_after_registered_save` ([MS-OXCROPS] section 2.2) | `<pass/fail/not run>` | `<pass/fail/not run>` | `<pass/fail/not run>` | `<artifact>` |

### Autodiscover Response

Record the sanitized responses returned to the test account.

| Probe | Result | Sanitized response or artifact |
| --- | --- | --- |
| POX default autodiscover | `<status/result>` | `<path to saved XML or excerpt>` |
| POX with `X-MapiHttpCapability: 1` | `<status/result>` | `<path to saved XML or excerpt>` |
| JSON `Protocol=AutoDiscoverV1` | `<status/result>` | `<path to saved JSON or excerpt>` |
| JSON `Protocol=EWS` | `<status/result>` | `<path to saved JSON or excerpt>` |
| JSON `Protocol=MapiHttp` | `<status/result>` | `<path to saved JSON or excerpt>` |

Confirm the response publishes only endpoints that are implemented, exposed,
and intentionally enabled for the gate being tested.

### RCA Result

| Field | Value |
| --- | --- |
| RCA product/test | `<Outlook Connectivity test name>` |
| RCA run timestamp | `<timestamp and timezone>` |
| RCA result | `<pass/fail>` |
| RCA report/export | `<artifact path or report id>` |
| Public host used by RCA | `<host>` |
| Account used by RCA | `<mailbox address>` |
| RCA failing step, if any | `<step text, error code, component, detection location>` |
| RCA correlation id, if any | `<id>` |

### Local Harness Result

| Field | Value |
| --- | --- |
| Command | `python tools/rca_outlook_connectivity_check.py --outlook-rca-readiness --base-url <url> --email <account> --expected-service-host <host> --allow-mutating-fixtures` |
| Additional flags | `<for example --insecure only for closed labs>` |
| Run timestamp | `<timestamp and timezone>` |
| Result | `<pass/fail>` |
| Output artifact | `<log path or CI run>` |
| Fixture cleanup confirmed | `<yes/no/not applicable>` |

### MAPI/HTTP Gate 1 Result

| Field | Value |
| --- | --- |
| Command | `LPE_RCA_PASSWORD='...' python tools/rca_outlook_connectivity_check.py --mapi-gate1-readiness --base-url https://<public-lpe-ct-host> --expected-service-host <public-lpe-ct-host> --email <disposable-mailbox>` |
| Run timestamp | `<timestamp and timezone>` |
| Result | `<pass/fail/not run>` |
| Sanitized POX result | `<mapiHttp Version 1, no EXCH/EXPR, published EMSMDB/NSPI URLs>` |
| EMSMDB/NSPI evidence | `<request ids, redacted cookie state, private-logon and hierarchy ROP results>` |
| Initial hierarchy rows | `<Inbox, Sent, Drafts, Deleted Items/Trash, Calendar, Contacts, Tasks, Notes, Journal>` |
| First failing step and correlation ids, if any | `<step, request ids, server/edge trace ids>` |

This harness is only bounded wire evidence. A pass does not prove a clean Outlook 2016 or Outlook 2019 profile reaches `Connected`, and does not prove the broader cached-mode, reconnect, NSPI-resolution, send/`Sent`, Microsoft RCA, or RPC/HTTP release gates.

### Real Outlook Checklist

Complete this checklist separately for Outlook 2016 and Outlook 2019.

| Step | Outlook 2016 result | Outlook 2019 result | Evidence |
| --- | --- | --- | --- |
| Clean Windows profile and clean Outlook profile used | `<pass/fail/not run>` | `<pass/fail/not run>` | `<artifact>` |
| Exchange profile created through documented autodiscover path | `<pass/fail/not run>` | `<pass/fail/not run>` | `<artifact>` |
| MAPI over HTTP selected for mailbox transport | `<pass/fail/not run>` | `<pass/fail/not run>` | `<artifact>` |
| Initial cached-mode sync completed for mail, calendar, contacts, tasks, notes, journal, and supported search/reminder folders | `<pass/fail/not run>` | `<pass/fail/not run>` | `<artifact>` |
| Outlook closed and reopened twice without OST deletion, profile repair, or full cache rebuild | `<pass/fail/not run>` | `<pass/fail/not run>` | `<artifact>` |
| Reopened session used checkpoint/delta behavior without duplicates, loss, or resurrection | `<pass/fail/not run>` | `<pass/fail/not run>` | `<artifact>` |
| NSPI resolved authenticated mailbox and visible contacts within tenant/account boundaries | `<pass/fail/not run>` | `<pass/fail/not run>` | `<artifact>` |
| Outlook send used canonical LPE submission | `<pass/fail/not run>` | `<pass/fail/not run>` | `<artifact>` |
| Sent item appeared in authoritative canonical `Sent` and matched supported non-MAPI protocols | `<pass/fail/not run>` | `<pass/fail/not run>` | `<artifact>` |
| Read/unread, flags, moves, copies, deletes, drafts, attachments, and protected `Bcc` metadata stayed consistent with canonical LPE state | `<pass/fail/not run>` | `<pass/fail/not run>` | `<artifact>` |

### Logs and Artifacts

| Artifact | Time window | Location or reference | Notes |
| --- | --- | --- | --- |
| LPE server logs | `<start/end, timezone>` | `<path, trace ids, log query>` | `<notes>` |
| LPE-CT edge logs | `<start/end, timezone>` | `<path, trace ids, log query>` | `<notes>` |
| Autodiscover request/response captures | `<start/end, timezone>` | `<path>` | `<redaction notes>` |
| MAPI EMSMDB traces | `<start/end, timezone>` | `<path, request ids>` | `<redaction notes>` |
| MAPI NSPI traces | `<start/end, timezone>` | `<path, request ids>` | `<redaction notes>` |
| Microsoft RCA report | `<timestamp>` | `<path or report id>` | `<notes>` |
| Outlook client logs | `<start/end, timezone>` | `<path>` | `<redaction notes>` |
| Screenshots or video | `<timestamp>` | `<path>` | `<notes>` |

## Reference Table/List

| Decision | Rule |
| --- | --- |
| Local harness pass | Does not imply Microsoft RCA pass or real Outlook profile pass. |
| MAPI/HTTP Gate 1 harness pass | Does not imply a real Outlook profile reaches `Connected`, or any broader release gate. |
| Microsoft RCA pass | Does not imply Outlook 2016 or Outlook 2019 cached-mode profile pass. |
| Outlook 2016 pass | Does not imply Outlook 2019 pass. |
| Outlook 2019 pass | Does not imply Outlook 2016 pass. |
| MAPI publication | Requires `LPE_AUTOCONFIG_MAPI_ENABLED`, `LPE_AUTOCONFIG_MAPI_INTEROP_GATE_PASSED`, and client capability negotiation; per [MS-OXDSCLI] sections 2.2.2.1 and 3.2.5.1, the header is not transport evidence. |
| This template | Records release evidence only; it does not change endpoint flags. |
| Legacy `EXPR` | Still requires its independent RPC proxy and interoperability gates. |
