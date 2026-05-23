# Outlook Cached-Mode Gate Evidence Template

## Current State/Functionality Overview

This template records evidence for the guarded Outlook 2016 and Outlook 2019
cached-mode `MAPI over HTTP` gate. It is an evidence capture artifact only. It
does not mark the gate passed, enable MAPI autodiscover, or set
`LPE_AUTOCONFIG_OUTLOOK_INTEROP_GATE_PASSED`.

Use one completed copy per deployment class and test window. Do not merge
Outlook 2016 and Outlook 2019 evidence; each client version needs its own real
profile result.

## Implementation/Usage

Complete the fields below before any publication decision. Redact passwords,
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
| Endpoint flags | `LPE_AUTOCONFIG_EWS_ENABLED=<true/false>; LPE_AUTOCONFIG_MAPI_ENABLED=<true/false>; LPE_AUTOCONFIG_OUTLOOK_INTEROP_GATE_PASSED=<false>` |

`LPE_AUTOCONFIG_OUTLOOK_INTEROP_GATE_PASSED` must remain `false` while this
template is being collected.

### Client Matrix

| Client | Outlook version/build | Windows build | Profile type | Cached mode | Result | Evidence |
| --- | --- | --- | --- | --- | --- | --- |
| Outlook 2016 | `<version/build/channel>` | `<Windows version/build>` | `<clean Exchange profile>` | `<enabled>` | `<pass/fail/not run>` | `<logs/screenshots/trace ids>` |
| Outlook 2019 | `<version/build/channel>` | `<Windows version/build>` | `<clean Exchange profile>` | `<enabled>` | `<pass/fail/not run>` | `<logs/screenshots/trace ids>` |

### Gate Results

| Gate | Required evidence | Result | Evidence reference |
| --- | --- | --- | --- |
| Local harness | `cargo test -p lpe-exchange` and `tools/rca_outlook_connectivity_check.py --outlook-rca-readiness` against the target deployment shape | `<pass/fail/not run>` | `<command output, CI run, trace ids>` |
| Microsoft RCA | Microsoft Remote Connectivity Analyzer Outlook Connectivity against the same public host, account, tenant, TLS certificate, and endpoint flags | `<pass/fail/not run>` | `<RCA timestamp, test name, correlation id, exported report>` |
| Outlook 2016 real profile | Clean Outlook 2016 Exchange profile creates, syncs cached mode, reopens twice, resolves NSPI, submits via canonical LPE submission, and shows canonical `Sent` | `<pass/fail/not run>` | `<client logs, screenshots, server trace ids>` |
| Outlook 2019 real profile | Clean Outlook 2019 Exchange profile creates, syncs cached mode, reopens twice, resolves NSPI, submits via canonical LPE submission, and shows canonical `Sent` | `<pass/fail/not run>` | `<client logs, screenshots, server trace ids>` |

All four rows must be independently `pass` before a publication decision can
consider setting `LPE_AUTOCONFIG_OUTLOOK_INTEROP_GATE_PASSED=true`.

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
| Microsoft RCA pass | Does not imply Outlook 2016 or Outlook 2019 cached-mode profile pass. |
| Outlook 2016 pass | Does not imply Outlook 2019 pass. |
| Outlook 2019 pass | Does not imply Outlook 2016 pass. |
| Publication | Requires local harness, Microsoft RCA, Outlook 2016, and Outlook 2019 evidence to pass, plus explicit endpoint flags. |
| This template | Records evidence only; it must not enable MAPI autodiscover or set `LPE_AUTOCONFIG_OUTLOOK_INTEROP_GATE_PASSED`. |
