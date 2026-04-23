# ActiveSync Interoperability Matrix

This document turns the current `ActiveSync` MVP scope into a concrete client matrix and test plan.

It is intentionally bounded to the commands, collections, auth modes, and behavioral guarantees already documented in `docs/architecture/activesync-mvp.md`. It does not widen the MVP to `EWS`, `Tasks`, richer `GAL` search, direct `SMTP`, or any non-canonical submission path.

In this document, `Outlook` desktop means classic `Outlook` for Windows configured as a direct `ActiveSync` client. `New Outlook` for Windows and `Outlook` for Mac are outside this MVP matrix because they are not the intended direct `ActiveSync` validation surface here.

### Scenario codes

| Code | Scenario | MVP area |
| --- | --- | --- |
| `S1` | Account enrollment with `Provision` and first `FolderSync` | auth, device policy, folder discovery |
| `S2` | Initial mailbox `Sync` with `SyncKey = 0`, priming round-trip, and first page | first sync correctness |
| `S3` | Incremental mailbox `Sync` for `Inbox`, `Sent`, and `Drafts` | steady-state mail sync |
| `S4` | `Drafts` create, update, delete through `Sync` | draft persistence |
| `S5` | `SendMail` with canonical `Sent` visibility after submission | send-flow correctness |
| `S6` | `SendMail` / `SmartReply` / `SmartForward` with common MIME bodies and attachments | reply or forward fidelity |
| `S7` | `ItemOperations` fetch for message body and attachment payload | fetch and attachment retrieval |
| `S8` | `Ping` wait, wake, and re-sync across synchronized folders | long-poll behavior |
| `S9` | `Search` against canonical mailbox and supported attachment text index | mailbox search |
| `S10` | Contacts and calendar `Sync` add, change, delete | collaboration collections |
| `S11` | Shared mailbox projection for delegated `Inbox`, `Sent`, `Drafts` | delegated folder sync |
| `S12` | Delegated `SendMail` / `SmartReply` / `SmartForward` with `send_as` and `send_on_behalf` semantics | delegated submission |

### Client matrix

| Client | Version band to lab against | Auth in scope | Protocol target | Required scenarios | Notes |
| --- | --- | --- | --- | --- | --- |
| `Outlook` desktop for Windows (classic Microsoft 365) | current supported Microsoft 365 production channel used by the lab | `Basic`, `Bearer` | `ActiveSync 16.1` target; verify actual negotiated behavior | `S1` `S2` `S3` `S4` `S5` `S6` `S7` `S8` `S11` `S12` | Highest-priority credibility client for delegated mail, drafts, send flow, and long-lived sync. |
| `Outlook` mobile for iOS | current App Store release used by the lab | `Bearer` first, `Basic` fallback if still exposed | `ActiveSync 16.1` target; verify actual negotiated behavior | `S1` `S2` `S3` `S5` `S6` `S7` `S8` | Focus on mobile send flow, attachment fetch, and reconnect stability rather than delegated folders. |
| `Outlook` mobile for Android | current Play Store release used by the lab | `Bearer` first, `Basic` fallback if still exposed | `ActiveSync 16.1` target; verify actual negotiated behavior | `S1` `S2` `S3` `S5` `S6` `S7` `S8` | Same priority as iOS mobile because MIME and long-poll behavior can diverge by platform shell. |
| `iOS Mail` | latest supported `iOS 18.x` device image used by the lab | `Basic`; `Bearer` if the client surface supports it in the chosen enrollment flow | `ActiveSync 16.1` target; verify actual negotiated behavior | `S1` `S2` `S3` `S5` `S6` `S7` `S8` `S10` | Strategic native-client target called out by the architecture; validate stable enrollment and day-two refresh. |
| `Samsung Email` on Samsung One UI / Android | latest supported Samsung Email release on the lab device image | `Basic` | `ActiveSync 16.1` target; verify actual negotiated behavior | `S1` `S2` `S3` `S5` `S7` `S8` `S10` | Chosen as the single Android-native `ActiveSync` client because generic Android mail support is fragmented. |

### Defect-risk priorities

1. `P0`: canonical `Sent` divergence after `SendMail`, `SmartReply`, or `SmartForward`. If any client can send successfully without the authoritative `Sent` copy appearing in the next sync, the MVP breaks a core architecture guarantee.
2. `P0`: unstable first sync and continuation behavior. The documented conservative `SyncKey = 0` priming round-trip and invalidatable paged continuation are high-risk because many real clients retry aggressively or treat continuation errors as account breakage.
3. `P0`: `Ping` and long-poll instability under long-lived sessions. The MVP explicitly treats `Ping` as lightweight today, so premature wakeups, stalled polls, or missed changes can make native clients appear unreliable even when manual refresh works.
4. `P1`: delegated mailbox projection and delegated submission drift. Shared `Inbox` or `Sent` visibility, `send_as`, and `send_on_behalf` semantics are credibility-critical for `Outlook` desktop even if mobile clients do not expose every path equally.
5. `P1`: draft mutation interoperability gaps. Draft synchronization is explicitly targeted at `ActiveSync 16.1`; any client that negotiates differently may partially work for mail sync but still break compose-save-send behavior.
6. `P1`: MIME parsing gaps on common native-client output. Folded headers, encoded names, `quoted-printable`, `base64`, and multipart bodies are in scope, but compose variations across Outlook and iOS remain a likely regression surface.
7. `P1`: folder identity or ordering drift in `FolderSync`. If `Inbox`, `Sent`, `Drafts`, `Contacts`, or `Calendar` are remapped inconsistently, downstream `Sync`, `Ping`, and UI behavior become noisy or incorrect.
8. `P2`: attachment fetch mismatch between `SendMail`, `Sync`, and `ItemOperations`. The MVP promises canonical attachment persistence and later retrieval, so broken `FileReference` handling would look like partial message corruption.
9. `P2`: search false negatives or data-shape mismatches. `Search` is intentionally limited, but supported mailbox search and the existing attachment text index still need deterministic behavior for MVP credibility.
10. `P2`: contacts and calendar mutation mismatch. These are in documented scope, but they should remain below mail-flow stabilization in priority because the architecture positions mailbox correctness as the flagship concern first.

### Recommended automated test cases

| Test id | Matrix scenarios | Recommended automation |
| --- | --- | --- |
| `AT1` | `S1` | Protocol-level enrollment test covering `OPTIONS`, `Provision`, policy acknowledgement, and first `FolderSync` for `Basic` and `Bearer`. |
| `AT2` | `S2` | Initial `SyncKey = 0` test that asserts conservative priming, first-page emission, stable `SyncKey` advancement, and no duplicate items after retry. |
| `AT3` | `S3` | Incremental mail sync test for add, change, delete across `Inbox`, `Sent`, and `Drafts`, including multi-collection requests and tolerated options. |
| `AT4` | `S4` | `Drafts` `Sync` mutation test for add, change, delete, then send, asserting canonical draft persistence and removal semantics. |
| `AT5` | `S5` | `SendMail` test asserting transactional canonical submission, authoritative `Sent` visibility, and outbound queue creation without direct `SMTP` bypass. |
| `AT6` | `S6` | MIME corpus test for `SendMail`, `SmartReply`, and `SmartForward` with folded headers, RFC 2047 names, `quoted-printable`, `base64`, `multipart/alternative`, and common attachments. |
| `AT7` | `S7` | `ItemOperations` fetch test for message body plus attachment payload by canonical `FileReference`, including payload integrity checks. |
| `AT8` | `S8` | Long-poll harness for `Ping` that verifies wait behavior, wake-on-change, timeout behavior, and immediate follow-up `Sync` convergence. |
| `AT9` | `S9` | Mailbox `Search` test over body, headers, and supported attachment text indexing while asserting that protected `Bcc` metadata is not surfaced. |
| `AT10` | `S10` | Contacts and calendar mutation round-trip test for add, change, delete through `Sync`, validating canonical storage writes and repeat sync stability. |
| `AT11` | `S11` | Delegated folder projection test for same-tenant shared `Inbox`, `Sent`, and `Drafts`, including folder discovery, sync visibility, and `Ping` wakeups. |
| `AT12` | `S12` | Delegated submission authorization test covering `send_as` and `send_on_behalf`, including sender identity, canonical `Sent`, and reply or forward behavior. |
| `AT13` | `S2` `S3` `S8` | Continuation invalidation resilience test where a mailbox item changes mid-pagination and the client is forced onto a fresh sync path without mailbox divergence. |
| `AT14` | `S1` `S3` `S5` | Cross-auth parity test proving the same mailbox state and submission outcomes for `Basic` and scoped `Bearer` auth where both are in documented scope. |

### Automation order

1. Gate every build with `AT2`, `AT3`, `AT5`, `AT8`, and `AT13`.
2. Run `AT4`, `AT6`, `AT7`, `AT11`, and `AT12` in the ActiveSync interoperability suite for every protocol-adapter change.
3. Run `AT1`, `AT9`, `AT10`, and `AT14` at least in nightly coverage and before any client-lab certification pass.
