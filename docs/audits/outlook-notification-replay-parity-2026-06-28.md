# Outlook Notification Replay Parity - 2026-06-28

## Scope

This follow-up tracks EWS and MAPI notification replay parity after the
maintenance audit. It is documentation-only and does not enable additional
notification behavior.

Read with:

- `docs/architecture/ews-mapi-mvp.md`
- `docs/architecture/mapi-over-http-implementation-plan.md`
- `docs/architecture/ews-interoperability-matrix.md`
- `docs/architecture/outlook-exchange-parity-roadmap.md`
- `docs/architecture/sql-schema-v2.md`
- `docs/audits/lpe-maintenance-outlook-architecture-audit-2026-06-27.md`

## Current Behavior

LPE uses canonical change replay as the notification source of truth. Durable
mail and collaboration mutations write `mail_change_log` rows, and broader
canonical categories can also write `canonical_change_journal` rows. Protocol
adapters may shape those facts for clients, but they must not create a parallel
mailbox notification store.

Current Exchange-facing behavior:

- MAPI `RopRegisterNotification` registers session-scoped content or hierarchy
  watches for the supported bitmask.
- MAPI `NotificationWait` returns protocol-shaped no-event or event-pending
  responses from registered session events and canonical `mail_change_log`
  replay.
- MAPI pending registrations and event queues are in-memory session state.
  Clients must re-register after reconnect, process restart, or worker movement.
- EWS `Subscribe` creates deterministic pull subscription ids and watermarks
  seeded from the canonical replay cursor.
- EWS `GetEvents` replays canonical `mail_change_log` events and returns
  parseable expiry errors for stale watermarks.
- EWS `GetStreamingEvents` uses the same canonical replay path in a bounded
  streaming-shaped response.
- EWS `Unsubscribe` is a compatibility response because subscription truth is
  the canonical cursor/watermark, not a durable EWS-local row.

## Remaining Parity Gaps

| Gap | Current boundary | Why it matters to Outlook | Canonical model needed | Tests/evidence needed | Blocks public MAPI autodiscover? |
| --- | --- | --- | --- | --- | --- |
| Cross-process MAPI notification replay | MAPI registrations, pending event queues, and table notification state are session-local. Canonical `mail_change_log` lets clients resync after reconnect, but it does not restore live registrations after worker movement. | Outlook cached mode can recover through sync, but production deployments need predictable behavior if a worker restarts or load balancing moves a session. | Either a documented sticky-session deployment requirement or durable transport-session notification registration metadata that can be replayed without becoming mailbox truth. | Restart and worker-migration tests, reconnect tests proving normal sync convergence, and Outlook idle/change observation evidence. | No for the first sticky single-node lab gate if cached-mode sync converges; yes for non-sticky production publication. |
| Full MAPI notification payload parity | Current payloads are bounded content/hierarchy signals with folder/message identifiers, move source identifiers, change cursor, modseq, counts, object/change kind, display names, and message subject when available. Full Exchange table/row payload parity remains deferred. | Outlook may use notification payload details to refresh views efficiently without full table or ICS resync. Missing payload fields can cause stale views or extra resync traffic. | A canonical notification projection model that maps each supported Exchange payload field to durable canonical state or bounded compatibility metadata. | Payload golden tests, table-modified tests, contents/hierarchy delta tests, and real Outlook traces showing which fields are consumed. | Conditional. It blocks publication only if Outlook profile evidence shows stale or broken views without the wider payload. |
| EWS long-held streaming affinity | `GetStreamingEvents` returns a bounded streaming-shaped response over canonical replay. It does not implement full long-held Exchange streaming affinity or server-side subscription pinning. | EWS clients can depend on long-poll/streaming semantics for mailbox freshness. Outlook desktop MAPI publication may not require EWS streaming, but native/EWS clients can. | Canonical notification replay plus optional durable subscription/affinity metadata that records scope, timeout, reconnect behavior, and expiration without owning mailbox truth. | Long-poll timeout tests, reconnect tests, multi-worker affinity tests, and real EWS client evidence. | No for basic MAPI autodiscover unless Outlook desktop uses the published EWS profile path for notifications. |
| EWS push notifications | Push subscriptions remain unsupported. `Subscribe` is bounded to pull-style deterministic ids/watermarks and does not create an Exchange subscription table. | Some EWS integrations rely on server-to-client push. Implementing it incorrectly would create an outbound callback system with security and retry semantics outside current canonical notification replay. | A canonical push subscription model with endpoint validation, tenant/account authorization, retry/backoff, delivery audit, expiration, and security policy. | Push subscribe/request validation tests, delivery/retry tests, authz tests, and operational documentation. | No for MAPI autodiscover. It is an EWS/native-client parity and operations feature. |
| Notification retention and watermark expiry policy | EWS returns parseable invalid-watermark errors for expired cursors. MAPI clients rely on normal sync/checkpoint behavior when session-local notifications are unavailable. | Clients must recover deterministically when retained change rows have been purged. Ambiguous expiry can cause missed changes or repeated full sync. | Documented retention windows and recovery behavior for `mail_change_log`, `canonical_change_journal`, tombstones, EWS watermarks, and MAPI sync checkpoints. | Retention expiry tests, watermark invalidation tests, full-resync fallback tests, and documentation of operational retention settings. | Conditional. It blocks publication if Outlook evidence shows unrecoverable stale views after retention expiry. |
| Public-folder and shared-resource notification scope | Public-folder per-user state notifications are scoped to the account whose private state changed, and public-folder item/permission/replica changes use canonical replay. Shared mailbox/calendar notification breadth remains bounded by existing grant and change-log behavior. | Outlook shared folder, delegate, and public-folder views need notifications scoped to the right principals without leaking private state. | Canonical notification audience rules for mailbox grants, calendar/contact/task grants, public-folder grants, per-user private state, permission removal, and tenant boundaries. | Grant mutation tests, shared/delegate notification tests, public-folder per-user-state tests, permission-revocation replay tests, and Outlook shared-folder evidence. | No for private mailbox publication unless delegated/shared scenarios are part of the gate. |
| Spooler advisory event semantics | `RopSetSpooler`, `RopSpoolerLockMessage`, and `RopTransportNewMail` remain parseable unsupported probes and do not create notification state. | Outlook send/reconnect traces may include advisory probes; accepting them without a canonical model could corrupt transport or notification custody. | The spooler advisory model documented separately: optional canonical advisory state tied to submission events, LPE-CT handoff, and normal `mail_change_log` notifications. | Trace-driven advisory ROP tests if widened, canonical submission notification tests, and send/reconnect Outlook evidence. | Draft/send/Sent behavior can block publication; advisory notifications block only if real Outlook requires acknowledged advisory state. |

## Boundary Decision

The current notification boundary is correct for the architecture. Durable
mailbox and collaboration facts live in `mail_change_log`,
`canonical_change_journal`, tombstones, and canonical object tables. MAPI
session notifications are transport-session state; EWS subscription ids and
watermarks are compatibility projections over canonical replay.

Do not add an EWS-local subscription table or MAPI-local notification queue that
owns mailbox truth. Durable subscription or affinity metadata may be added only
if it is explicitly modeled as transport compatibility state and cannot shadow
canonical change replay.

## Verification Expectations

Before closing any notification parity gap:

- add focused MAPI `RopRegisterNotification` / `NotificationWait` tests for the
  new event shape or recovery behavior;
- add EWS `Subscribe`, `GetEvents`, `GetStreamingEvents`, or `Unsubscribe` tests
  for the widened behavior;
- prove restart, reconnect, stale watermark, and retention-expiry behavior;
- prove cross-protocol convergence through canonical sync after notification
  loss;
- attach real Outlook or EWS client evidence when a behavior is claimed to
  affect public MAPI autodiscover or native-client parity.

## Verification Performed

Commands used for this documentation follow-up:

- `rg -n "NotificationWait|RopRegisterNotification|RegisterNotification|pending notification|mail_change_log|canonical_change_journal|Subscribe|GetEvents|GetStreamingEvents|Unsubscribe|notification replay|streaming|push notification|cross-process notification" docs/architecture docs/audits crates/lpe-exchange/src crates/lpe-storage/src -g "*.md" -g "*.rs"`
