# MAPI/HTTP Outlook Cached-Mode Audit 2026-05-31

## Current State/Functionality Overview

This audit compares the provided Outlook cached-mode trace and `LPE` server log
against the documented guarded MAPI/HTTP publication gate. It records observed
behavior only; it does not mark the MAPI/HTTP gate passed and does not authorize
public autodiscover publication.

Artifacts reviewed:

- `C:\Users\dedu\AppData\Local\Temp\Outlook Logging\OUTLOOK_16_0_20026_20112-20260531T1027120156-26336.etl`
- `logs\LPE_last_202605311028.log`

The ETL converts with `tracerpt` and confirms an Outlook logging artifact, but
the generic CSV conversion exposes mostly provider/event metadata rather than
decoded MAPI request payloads. The server log is the authoritative protocol
evidence for this audit.

## Evidence Summary

| Area | Observed evidence | Gate comparison |
| --- | --- | --- |
| Outlook client | Server logs show `Microsoft Outlook 16.0.20026` and `Outlook/16.0.20026.20076` during the 2026-05-31 run. | This is one Outlook 16.0 build trace. The artifact does not prove separate Outlook 2016 and Outlook 2019 cached-mode passes. |
| Microsoft RCA | No Microsoft Remote Connectivity Analyzer exported report, correlation id, or result artifact was provided. | RCA gate remains unproven. Server log entries named `rca debug` are LPE diagnostics and are not Microsoft RCA output. |
| Autodiscover | One POX autodiscover request at `2026-05-31T08:27:44Z` for `test@l-p-e.ch` returned `IMAP`, `SMTP`, `WEB`, and `Protocol Type="mapiHttp"` with EMSMDB and NSPI URLs. No top-level `EXPR` block was observed. | The response shape matches the MAPI/HTTP endpoint shape, but publication is gate-compliant only when the deployment flags and all local, RCA, Outlook 2016, and Outlook 2019 evidence are complete. |
| NSPI calls | Observed `Bind`, `DNToMId`, `GetProps`, `GetSpecialTable`, `GetMatches`, and `Unbind`. `DNToMId` resolved the authenticated mailbox to `0x8000003f`; `GetMatches` returned `0x8000003f:account:test@l-p-e.ch:test`. | Covered by the documented NSPI bootstrap/address-book scope. NSPI mutation was not observed. |
| EMSMDB calls | Observed `Connect`, `Execute`, and `Disconnect`; authenticated calls returned MAPI response code `0`. Initial unauthenticated `Bind`/`Connect` probes returned HTTP `401` with Basic challenge. | Covered by the documented authenticated MAPI/HTTP transport gate. |
| Cookies | Successful session establishment returned `MapiContext,MapiSequence`. Later calls selected matching context/sequence hashes for each session. | Matches the documented single-node sticky-session cookie model. Cross-process replay remains outside this evidence. |
| Observed ROPs | `Logon`, `GetAddressTypes`, `GetReceiveFolder`, `GetLocalReplicaIds`, `OpenFolder`, `GetPropertiesSpecific`, `GetPropertyIdsFromNames`, `SetProperties`, `SynchronizationConfigure`, `SynchronizationUploadStateStreamBegin`, `SynchronizationUploadStateStreamContinue`, `SynchronizationUploadStateStreamEnd`, `FastTransferSourceGetBuffer`, and `Release`. | All observed ROPs are in the documented bounded profile/sync surface. No new ROP family is implied by this trace. |
| Sync checkpoints | Content sync checkpoints were stored for `ipm_subtree`, `inbox`, `drafts`, `contacts`, `calendar`, `journal`, `notes`, `tasks`, `sent`, `trash`, and projected Outlook folders. The log records usable checkpoints and `checkpoint_store_status="ok"` after drained buffers. | Matches the documented durable `mapi_sync_checkpoints` model for cached-mode sync. |
| Notifications | No `NotificationWait` or `RegisterNotification` call was observed in the provided log window. | Notification support remains covered by local tests, not by this trace. |
| Terminal result | The provided server log window ends during cached-mode synchronization and contains no canonical Outlook send proof, canonical `Sent` cross-protocol proof, close/reopen-twice proof, or second Outlook-version proof. | The real-Outlook gate remains incomplete. |

## Interoperability Matrix Update

| Gate item | Current evidence status | Required before publication |
| --- | --- | --- |
| Local harness | Not evaluated by this audit. | Keep requiring `cargo test -p lpe-exchange` and `tools/rca_outlook_connectivity_check.py --outlook-rca-readiness` for the target deployment. |
| Microsoft RCA Outlook Connectivity | Missing from provided artifacts. | Provide the exported RCA report/result for the same host, account, tenant, TLS certificate, and endpoint flags. |
| Outlook 2016 cached mode | Not proven. | Provide a separate Outlook 2016 clean-profile trace and server log showing profile creation, cached sync, reopen behavior, NSPI, canonical send, and canonical `Sent`. |
| Outlook 2019 cached mode | Not proven. | Provide a separate Outlook 2019 clean-profile trace and server log showing profile creation, cached sync, reopen behavior, NSPI, canonical send, and canonical `Sent`. |
| Outlook 16.0.20026 trace | Partially proven. | Keep as useful implementation evidence for MAPI/HTTP profile/sync behavior, but do not substitute it for the required version-specific gate rows. |
| Autodiscover publication | Observed `mapiHttp` publication and no `EXPR` publication in the provided log. | Confirm deployment flags and complete all publication evidence before setting `LPE_AUTOCONFIG_OUTLOOK_INTEROP_GATE_PASSED=true` for a public deployment. |
