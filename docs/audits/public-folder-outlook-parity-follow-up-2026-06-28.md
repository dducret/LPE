# Public Folder Outlook Parity Follow-Up - 2026-06-28

## Scope

This follow-up tracks public-folder Outlook and Exchange parity after the
maintenance audit. It is documentation-only and does not enable additional MAPI
or EWS behavior.

Read with:

- `docs/architecture/public-folders-mapi-mvp.md`
- `docs/architecture/mapi-over-http-implementation-plan.md`
- `docs/architecture/ews-mapi-mvp.md`
- `docs/architecture/outlook-exchange-parity-roadmap.md`
- `docs/audits/lpe-maintenance-outlook-architecture-audit-2026-06-27.md`

## Current Behavior

LPE now has a bounded canonical public-folder model. Public-folder trees,
folders, post-like items, permissions, replicas, per-user read state, replay,
and tombstones are represented as canonical state rather than MAPI-local state.

The current Exchange adapters project that model as follows:

- EWS exposes bounded public-folder folder discovery, folder create/delete,
  item list/read/sync, post create/update/delete, and public-folder-to-public-
  folder copy/move.
- MAPI over HTTP supports public-folder `RopLogon`, guarded hierarchy probes,
  canonical root and child hierarchy projection, contents-table projection for
  post-like items, folder and item property reads, content sync export,
  bounded post create/update/delete/copy/move, permission table projection and
  same-tenant permission mutation, replica owner lookup, ghost-state
  derivation, bounded per-user information streams, and per-user read/unread
  mutation.
- Public-folder rules are intentionally absent. `RopGetRulesTable` returns an
  empty canonical table for public-folder handles instead of creating MAPI-local
  rules.
- Public-folder message status bits are session-local compatibility state and
  are not persisted as canonical item state.

## Remaining Parity Gaps

| Gap | Current boundary | Why it matters to Outlook | Canonical model needed | Tests/evidence needed | Blocks public MAPI autodiscover? |
| --- | --- | --- | --- | --- | --- |
| Exchange-compatible cross-server public-folder replication | `public_folder_replicas` records active owner server names and drives `RopGetOwningServers` / `RopPublicFolderIsGhosted`, but it is topology metadata only. | Outlook public-folder clients can probe ownership and ghosting; multi-server Exchange deployments use replication semantics beyond owner-name projection. | A canonical public-folder replica topology and replay model that distinguishes advertised owner metadata from actual replicated content custody, conflict handling, and failover. | MAPI replica/ghost tests for current owner projection, plus future multi-owner replication tests and Outlook public-folder trace evidence before widening behavior. | No for private mailbox MAPI publication. Yes only for a public-folder replication compatibility claim. |
| Recipient-bearing public-folder item conversion | MAPI `RopCreateMessage` / `RopSaveChangesMessage` supports bounded recipient-free `IPM.Post`-like items. Recipient-bearing public-folder creates are rejected before writing `public_folder_items`. EWS public-folder item operations are likewise bounded to supported canonical item ids. | Exchange public folders can contain mail-like posts and workflows that carry recipients or conversion semantics. Outlook may attempt recipient-bearing saves depending on item class and compose path. | A documented canonical conversion rule for message-to-public-folder and public-folder-to-message/item transitions, including recipient storage, protected Bcc handling, sender identity, item class, and whether the result is mailbox mail, public-folder post, or another collaboration object. | Rejection tests must remain until the model exists. Future tests need MAPI and EWS create/copy/move/import coverage, Bcc protection checks, cross-protocol visibility checks, and real Outlook traces for recipient-bearing public-folder compose. | No for private mailbox MAPI publication unless Outlook profile tests require this path. Yes for full public-folder compose parity. |
| Arbitrary Exchange per-user binary blobs | `RopReadPerUserInformation` / `RopWritePerUserInformation` round-trip a bounded single-chunk LPE-owned stream derived from canonical `public_folder_per_user_state`. Arbitrary Exchange-compatible blobs are rejected. | Outlook may use legacy per-user information streams for private read/unread or view-related state. Accepting unknown blobs blindly would create protocol-local state with unclear semantics. | A canonical per-user public-folder metadata model that names each accepted fact, owner account, scope, lifecycle, replay behavior, privacy boundary, and projection rules. Opaque Exchange blobs must not become active behavior by default. | Current bounded stream round-trip and rejection tests, plus trace-driven tests for any accepted additional blob shape. Cross-protocol tests must prove user-private state does not become shared item state. | No for private mailbox MAPI publication. Conditional for public-folder Outlook parity if real Outlook requires a specific blob shape. |
| Public-folder reparenting and move-folder parity | EWS public-folder create/delete/copy is bounded; public-folder move remains rejected until canonical reparenting exists. MAPI folder move/copy is bounded to user-created mailbox folders and rejects public folders. | Outlook public-folder administration can move folder subtrees. Incorrect partial support could corrupt hierarchy replay, permissions, or tombstones. | Canonical public-folder reparenting with cycle checks, path/sort updates, permission inheritance policy, replica and per-user-state preservation, change-log replay, and tombstones. | Public-folder move/reparent tests, hierarchy sync replay tests, permission-retention tests, and Outlook admin/client evidence. | No for private mailbox MAPI publication; yes for public-folder administration parity. |
| Whole-folder public-folder purge | Item-scoped public-folder delete/hard-delete is canonical. Whole-folder purge returns a parseable not-supported ROP error; public-folder deletion is conservative and requires empty active children/items. | Outlook cleanup and administrative workflows may attempt recursive or whole-folder purge. | Canonical lifecycle semantics for recursive public-folder deletion, item tombstones, permission tombstones, per-user-state cleanup, replica cleanup, retention/legal hold interaction, and replay ordering. | Whole-folder purge unsupported tests today; future recursive delete/purge tests across EWS, MAPI, replay, permissions, and tombstones. | No for private mailbox MAPI publication. Conditional for public-folder administration parity. |
| Full Exchange public-folder item-class parity | Current support is for bounded post-like public-folder items and selected property projections. Arbitrary Exchange item classes, embedded message edge cases, and full property-bag parity remain outside the current public-folder model. | Outlook can expose different public-folder item classes and expects stable property behavior across sync, open, edit, and copy. | Canonical public-folder object families for each supported class, property mapping rules, body/attachment handling, custom property persistence boundaries, and class-specific sync facts. | Class-by-class MAPI/EWS create/read/update/delete/copy/sync tests, property golden tests, attachment/body tests, and Outlook public-folder client evidence. | No for private mailbox MAPI publication; yes for any class included in a public-folder support claim. |

## Boundary Decision

The current public-folder implementation is correctly bounded: it projects
canonical LPE public-folder state and rejects or limits Exchange behaviors that
would otherwise introduce MAPI-local truth. The remaining gaps should be treated
as staged parity work, not permanent product exclusions.

Public MAPI autodiscover for the private-mailbox Outlook profile path should
not wait for full public-folder replication, recipient-bearing public-folder
conversion, arbitrary per-user blobs, public-folder reparenting, or whole-folder
public-folder purge unless the publication claim includes public-folder parity
or real Outlook profile evidence shows one of these paths is required during
private mailbox bootstrap.

## Verification Expectations

Before closing any public-folder parity gap:

- update `docs/architecture/public-folders-mapi-mvp.md` and
  `docs/architecture/outlook-exchange-parity-roadmap.md` with the canonical
  model and evidence;
- add focused MAPI and EWS tests for the behavior;
- add cross-protocol checks where the behavior changes canonical user-visible
  state;
- preserve tests proving unsupported Exchange-only blobs or unsupported
  recursive operations do not create protocol-local state;
- attach real Outlook trace evidence for any behavior claimed as required by
  Outlook.

## Verification Performed

Commands used for this documentation follow-up:

- `rg -n "public[- ]folder|PublicFolder|RopGetOwningServers|RopPublicFolderIsGhosted|per-user|recipient-bearing|replica|ghost|PublicFolder|RopLogon" docs/architecture docs/audits crates/lpe-exchange/src -g "*.md" -g "*.rs"`
