# MAPI Spooler Advisory Model

This note defines the current MAPI transport/spooler boundary for Outlook
compatibility. It is documentation-only and does not implement new ROP behavior.

## Current Behavior

`RopSubmitMessage` and `RopTransportSend` are implemented as canonical LPE
submission paths:

- pending messages are converted into canonical submission input;
- opened drafts or outbox messages are submitted through the existing
  draft/submission service path;
- protected `Bcc` metadata is loaded from canonical protected storage before
  draft submission;
- attachments are loaded from canonical attachment storage;
- the submitted message is visible through authoritative canonical `Sent`;
- the handle is rebound to the submitted canonical message identity.

`RopAbortSubmit` is implemented only as canonical queue cancellation. It resolves
the requested folder/message identity to the authenticated account's canonical
`Sent` membership and then to the matching `submission_queue` row. Only
pre-handoff statuses `queued`, `ready`, and `deferred` may transition to
terminal `cancelled`. Already-cancelled rows are idempotent. Handed-off,
relayed, bounced, failed, missing, or non-Sent rows return protocol errors
without deleting mail or recalling transport.

The following ROPs are parseable but unsupported:

- `RopSetSpooler`
- `RopSpoolerLockMessage`
- `RopTransportNewMail`

They are parsed to documented request boundaries so later ROP bytes are not
misaligned, then return ROP-specific protocol errors without canonical mailbox,
submission, or transport side effects.

## Boundary

LPE must not create Exchange-style client-spooler custody as protocol-local MAPI
state. The current ownership model is:

- core LPE owns canonical draft, submitted message, authoritative `Sent`,
  sender rights, protected `Bcc`, `submission_queue`, and `submission_events`;
- LPE-CT owns outbound relay custody after authenticated handoff;
- MAPI owns only protocol parsing, response mapping, session handles, and
  Outlook compatibility projection.

Spooler advisory ROPs must not:

- create a MAPI-local Outbox or Sent state;
- mark a client spooler active in durable storage;
- lock canonical messages or queue rows without a shared queue lease model;
- create or announce inbound mail;
- recall or mutate handed-off transport custody;
- bypass LPE-CT for client SMTP submission;
- expose protected `Bcc` through advisory diagnostics or notifications.

## Canonical Model Needed For Wider Advisory Support

Any future advisory support needs a canonical model before implementation:

- explicit advisory event types for client-spooler presence, lock attempts, and
  outbound/inbound delivery hints;
- queue lease or advisory lock semantics coordinated with the outbound worker
  and LPE-CT handoff;
- change-log or notification replay behavior so reconnects and cross-process
  sessions do not lose required advisory state;
- audit and tenant/account isolation rules;
- tests proving advisory acknowledgements do not duplicate sent mail, hide
  canonical `Sent`, leak `Bcc`, or alter LPE-CT custody.

Session-local no-op acknowledgement is acceptable only if real Outlook traces
prove Outlook requires success but does not depend on durable advisory state.

## Tests And Evidence

Existing evidence covers the current boundary:

- `mapi_over_http_microsoft_transport_spooler_rops_keep_batch_aligned_without_mutation`
- `mapi_over_http_microsoft_abort_submit_cancels_pre_handoff_submission`
- `mapi_over_http_microsoft_abort_submit_rejects_handed_off_and_terminal_submissions`
- MAPI submit and transport-send tests that assert canonical `Sent` visibility
  and protected `Bcc` handling
- storage `cancel_queued_submission` behavior, which updates
  `submission_queue`, writes a `submission_events.cancelled` row, writes
  canonical `mail_change_log`, and emits normal mail-change notifications

Before implementing advisory acknowledgements, add evidence for:

- real Outlook 2016 and Outlook 2019 traces showing the advisory ROP is required;
- duplicate/replayed Execute requests not duplicating `Sent` or queue rows;
- handed-off LPE-CT submissions remaining non-cancellable;
- notification/reconnect behavior if advisory state becomes durable;
- cross-protocol visibility through JMAP, IMAP where applicable, EWS, and MAPI.

## Public MAPI Autodiscover Impact

Draft, send, transport-send, abort-submit, authoritative `Sent`, and protected
recipient behavior are public MAPI autodiscover gate concerns.

`RopSetSpooler`, `RopSpoolerLockMessage`, and `RopTransportNewMail` do not block
public MAPI autodiscover unless real Outlook evidence shows supported Outlook
versions require acknowledged advisory behavior during profile creation,
cached-mode send, reconnect, or shutdown.
