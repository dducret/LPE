# Internationalized Mailbox Names

Accessed standards date: 2026-05-13.

## Purpose

This document defines the shared policy for user-visible Unicode mailbox names
in `LPE`. It is the implementation contract for future IMAP, JMAP, storage, and
UI work. It does not enable the feature by itself and does not change protocol
advertising until the required parser, storage, validation, and tests exist.

The policy is intentionally stricter than the protocol minimums. RFC 9051
defines IMAP mailbox naming, hierarchy, and `LIST` behavior. RFC 8621 defines
JMAP `Mailbox` object behavior and requires mailbox `name` values to be
Net-Unicode strings from RFC 5198. LPE adds one canonical normalization,
comparison, hierarchy, reserved-name, and spoofing policy so all protocol
adapters resolve the same mailbox in the same way.

## Authoritative References

| Reference | Sections | LPE use |
| --- | --- | --- |
| RFC 9051: Internet Message Access Protocol Version 4rev2, https://www.rfc-editor.org/rfc/rfc9051.html | 5.1, 5.1.1, 6.3.1, 6.3.4-6.3.11, 7.3.1, Appendix A | IMAP UTF-8 mailbox names, hierarchy delimiter behavior, `ENABLE`, `CREATE`, `DELETE`, `RENAME`, `SUBSCRIBE`, `UNSUBSCRIBE`, `LIST`, `STATUS`, and `LIST` response attributes. |
| RFC 8621: JMAP for Mail, https://www.rfc-editor.org/rfc/rfc8621.html | 2 | JMAP `Mailbox` `name`, `parentId`, `role`, `sortOrder`, `myRights`, and `isSubscribed` behavior. |
| RFC 5198: Unicode Format for Network Interchange, https://www.rfc-editor.org/rfc/rfc5198.html | 2, 3 | Net-Unicode baseline: UTF-8, Unicode scalar values, and NFC as the recommended interchange normalization. |
| Unicode Standard Annex #15: Unicode Normalization Forms, https://www.unicode.org/reports/tr15/ | all | NFC normalization and canonical-equivalence handling. |
| Unicode Technical Standard #39: Unicode Security Mechanisms, https://www.unicode.org/reports/tr39/ | confusables, mixed-script detection, restriction levels | Spoofing defense for mixed-script, whole-script, and confusable names. |
| Unicode Standard Annex #9: Unicode Bidirectional Algorithm, https://www.unicode.org/reports/tr9/ | 3, 4.3, conformance clauses | Bidi display risk and rejection of unsafe directional controls. |
| RFC 6154: IMAP LIST Extension for Special-Use Mailboxes, https://www.rfc-editor.org/rfc/rfc6154.html | 2, 3 | Special-use role attributes such as `\Sent`, `\Drafts`, `\Trash`, `\Junk`, and `\Archive`. |

## Settled Product Decisions

- Stored mailbox display names are Unicode NFC strings.
- The stored display name is the user-visible spelling after NFC
  normalization; LPE must not store both original unnormalized input and a
  normalized display variant.
- Sibling collision checks use a generated canonical comparison key, not
  PostgreSQL `lower(...)`.
- Canonical comparison keys are scoped to sibling sets under one parent mailbox
  and one account-visible mailbox namespace.
- `INBOX` is reserved case-insensitively and always resolves to the canonical
  inbox role in IMAP.
- Canonical special-use mailboxes keep stable backend names and role metadata,
  such as `INBOX`, `Sent`, `Drafts`, `Trash`, `Junk`, and `Archive`.
- Localized mailbox labels are client UI presentation, not backend identity.
- IMAP exposes special-use identity through `LIST` attributes, not localized
  display-name guessing.
- JMAP exposes special-use identity through `Mailbox.role`, not localized
  display-name guessing.
- `/` is LPE's IMAP hierarchy delimiter for mailbox paths.
- `/` is not allowed inside a mailbox-name segment.
- JMAP `parentId` hierarchy is required in the first internationalized mailbox
  release.
- IMAP `UTF8=ACCEPT` is included in the first internationalized mailbox release
  and must be advertised only after implemented behavior and tests match this
  document.
- IMAP `SUBSCRIBE`, `UNSUBSCRIBE`, `LIST (SUBSCRIBED)`, and compatibility
  `LSUB` behavior use canonical persisted subscription state.
- JMAP `isSubscribed` uses the same canonical persisted subscription state and
  is stored separately per user where a mailbox is shared, as required by RFC
  8621 section 2.
- Strict mixed-script and confusable validation is required before accepting
  new or renamed user-visible mailbox names.

## Data Model Contract

Future implementation should expose a single shared mailbox-name module, owned
by the canonical mailbox domain/storage layer and reused by every protocol
adapter. The module should provide these concepts:

| Concept | Required behavior |
| --- | --- |
| `MailboxDisplayName` | A validated, user-visible, NFC string for one hierarchy segment. |
| `MailboxSegment` | A validated segment that cannot be empty and cannot contain the hierarchy delimiter. |
| `MailboxPath` | An ordered list of decoded `MailboxSegment` values for IMAP-style path input. |
| `MailboxCanonicalKey` | A generated comparison key used for uniqueness, reserved-name checks, and spoofing comparison. |
| `MailboxNamePolicy` | The normalization, validation, reserved-name, delimiter, and collision policy in this document. |
| `ImapMailboxName` | Parser and serializer for IMAP UTF-8 mailbox names and hierarchy paths. |
| `JmapMailboxName` | JMAP validation adapter that applies the same `MailboxNamePolicy` to `Mailbox/set`. |

Protocol adapters must not implement private mailbox-name normalization or
duplicate detection. Storage must persist enough data to enforce sibling
canonical-key uniqueness atomically.

Mailbox display names must never be used as filesystem paths. Any durable blob,
export, or future filesystem placement must use opaque mailbox ids.

## NFC Storage and Display Policy

Input accepted by mailbox creation or rename is parsed as a Unicode string and
normalized to NFC before persistence. RFC 5198 defines Net-Unicode around UTF-8
and recommends NFC for network interchange. UAX #15 defines NFC as canonical
decomposition followed by canonical composition, preserving compatibility
distinctions that NFKC would erase.

LPE policy:

- Accept only valid Unicode scalar values after protocol decoding.
- Reject invalid UTF-8 before Unicode validation.
- Normalize accepted display names to NFC.
- Store only the NFC display form.
- Return only the stored NFC display form through IMAP, JMAP, web APIs, export,
  audit messages that include mailbox labels, and diagnostics.
- Treat `Cafe\u0301` and `Café` as the same display name for collision purposes
  because they normalize to the same NFC string.
- Do not use NFKC or NFKD for display storage. Compatibility folds can collapse
  intentional user-visible distinctions and are reserved only for security
  skeleton or confusable checks.
- Do not store unassigned Unicode code points or private-use code points in
  mailbox display names. RFC 5198 warns that Net-Unicode stability depends on
  assigned characters and that private-use code points should be avoided for
  Internet interchange.

## Canonical Comparison Key Policy

The canonical comparison key is generated from the NFC display name and used for
server-side equality, uniqueness, reserved-name, and spoofing checks. It is not
shown to users and is not a protocol field.

Key generation must be deterministic for a fixed Unicode data version:

1. Decode protocol input to Unicode.
2. Normalize to NFC.
3. Apply Unicode full case folding for equality.
4. Apply the same delimiter and whitespace boundary rules used for validation.
5. Generate a security skeleton for spoofing comparison using UTS #39 data.

Uniqueness rules:

- Two mailboxes with the same parent in the same account-visible namespace must
  not have the same canonical equality key.
- A mailbox cannot be created or renamed if its equality key collides with a
  sibling.
- A mailbox cannot be created or renamed if its UTS #39 security skeleton is
  confusable with a reserved role name or an existing sibling under strict
  validation.
- The canonical inbox is resolved by role, and any case-folded `inbox` spelling
  in IMAP maps to that mailbox instead of creating a user mailbox.
- Canonical keys are not locale-dependent. Locale-specific ordering may be used
  for JMAP `sortOrder` tie-break display, but not for equality.

The case-folding and skeleton steps are LPE policy. RFC 9051 and RFC 8621 do
not define Unicode equality for mailbox names beyond protocol syntax and
sibling uniqueness, so the shared module must own this behavior explicitly.

## Reserved Names and Special-Use Behavior

Reserved identities are role-bound, not spelling-bound:

| Role | Backend display name | IMAP special-use | JMAP role |
| --- | --- | --- | --- |
| inbox | `INBOX` | implicit IMAP inbox behavior | `inbox` |
| sent | `Sent` | `\Sent` | `sent` |
| drafts | `Drafts` | `\Drafts` | `drafts` |
| trash | `Trash` | `\Trash` | `trash` |
| junk | `Junk` | `\Junk` | `junk` |
| archive | `Archive` | `\Archive` | `archive` |

Rules:

- `INBOX` is not a normal user-created mailbox name. In IMAP, any
  case-insensitive reference to `INBOX` resolves to the canonical inbox, as
  required by IMAP mailbox naming behavior in RFC 9051 section 5.1.
- User-created top-level mailboxes must not have equality keys or security
  skeletons that collide with reserved role names or accepted compatibility
  aliases for those roles.
- Existing canonical special-use mailboxes may use the backend display names in
  the table above.
- Renaming a special-use mailbox does not change its role unless a separate
  role-management feature explicitly permits that operation.
- Localized names such as French or German labels for Inbox, Sent, Drafts, or
  Trash must be produced by clients from IMAP special-use attributes or JMAP
  `role`. They are not stored as canonical backend identities.
- LPE may keep compatibility aliases such as `Deleted Items` resolving to the
  canonical trash role where existing architecture already requires that
  behavior. Such aliases are reserved for spoofing checks and cannot be created
  as separate user mailboxes at the same hierarchy level.
- LPE does not support IMAP `CREATE-SPECIAL-USE` until a separate architecture
  update defines role assignment permissions and conflict handling. RFC 6154
  makes that CREATE parameter optional.

## Delimiter and Hierarchy Behavior

IMAP:

- The hierarchy delimiter is `/`.
- IMAP input mailbox paths are decoded into `MailboxPath` segments before
  validation.
- Empty path input is invalid for create, rename target, delete, status, select,
  subscribe, and unsubscribe operations unless the specific RFC 9051 command
  uses an empty reference name, as `LIST` does.
- Empty hierarchy segments are invalid. `Projects//2026`, `/Projects`, and
  `Projects/` are rejected for mailbox creation and rename targets.
- `/` is protocol syntax and cannot appear inside a stored segment.
- The selected mailbox name and `LIST` response name are serialized from stored
  segments joined with `/`.
- `%` and `*` are `LIST` pattern wildcards, not stored name characters when
  they occur in a pattern. `%` matches one hierarchy level. `*` matches
  recursively, including hierarchy delimiters, following RFC 9051 `LIST`
  semantics.
- Literal `%` or `*` characters in mailbox names are permitted only if the
  parser and serializer can distinguish literal names from pattern input for
  the command being processed. Until that distinction is tested, creation of
  names containing `%` or `*` should be rejected with a clear validation error.

JMAP:

- JMAP stores hierarchy through `parentId`, not through `/` in `name`.
- A JMAP mailbox `name` is exactly one validated `MailboxSegment`.
- JMAP create and update reject `/` in `name`.
- JMAP create and update validate that `parentId` belongs to the same account
  namespace, is visible to the acting user, grants child-creation or rename
  rights as applicable, and does not create a cycle.
- JMAP `Mailbox/query` tree behavior must use `parentId`; it must not infer
  hierarchy from slashes in names.

## Validation and Security Rejection Rules

Reject mailbox names or path segments with:

- invalid UTF-8 or non-scalar Unicode input,
- empty names,
- empty path segments,
- leading or trailing ASCII whitespace,
- C0 or C1 controls,
- CR, LF, TAB, and NUL,
- DEL,
- unassigned Unicode code points,
- private-use code points,
- surrogate code points,
- default-ignorable code points except for a documented allowlist,
- bidi override, embedding, and isolate controls,
- variation selectors unless explicitly allowlisted for emoji mailbox names,
- the hierarchy delimiter `/`,
- names exceeding the configured character or UTF-8 byte limit,
- names whose NFC form is empty or different only by stripped/ignored unsafe
  characters,
- names whose canonical equality key collides with a sibling,
- names whose canonical equality key collides with a reserved role identity or
  compatibility alias,
- names whose UTS #39 skeleton is confusable with a sibling or reserved role,
- mixed-script confusables,
- whole-script confusables,
- names that require a higher-level bidi protocol for safe interpretation.

Allowed examples:

- `Café`
- `案件`
- `📁 Projects`
- a single-script Arabic or Hebrew name without explicit bidi controls

Rejected examples:

- `Cafe\u0301` when `Café` already exists as a sibling,
- `Projects/2026` as a JMAP `name`,
- `Projects//2026` as an IMAP creation target,
- `INBOX` as a user-created mailbox,
- `pаypаl` using Cyrillic letters to spoof Latin `paypal`,
- names containing U+202E RIGHT-TO-LEFT OVERRIDE or other explicit bidi
  formatting controls,
- names containing zero-width controls unless a later architecture update adds a
  narrow allowlist.

Strict confusable validation is intentionally conservative. It can reject some
legitimate multilingual names. User-facing errors must explain the class of
problem without requiring users to understand Unicode internals, for example:
`Mailbox name is too similar to an existing folder`, `Mailbox name contains an
unsafe invisible character`, or `Mailbox name mixes scripts in a way that could
be confused with another folder`.

Implementation uses the `unicode-security` crate for UTS #39 skeleton and
mixed-script checks. `unicode-security` 0.1.2 is generated from Unicode 16.0
data, so LPE retains a narrow current-UTS #39 correction for U+04CF CYRILLIC
SMALL LETTER PALOCHKA until the crate ships the current mapping to Latin `l`.

## Bidi Display Policy

LPE stores mailbox names in logical Unicode order and relies on normal Unicode
rendering for display. UAX #9 defines bidi ordering and permits higher-level
protocols to apply directional context, but mailbox names appear in plain IMAP,
JSON, logs, web UI, mobile UI, and native clients where display context varies.

Rules:

- Do not accept explicit bidi formatting controls in mailbox names.
- Do not accept names whose safe display requires hidden directional controls.
- Treat each mailbox segment as an independent bidi display segment in the web
  UI and diagnostics.
- UI layers should render mailbox labels in an isolated text context and avoid
  concatenating raw mailbox names with punctuation or security-sensitive labels
  without isolation.
- Logs and audit messages should include mailbox ids alongside display names so
  bidi rendering cannot obscure identity.

## IMAP Contract

IMAP internationalized mailbox support targets IMAP4rev2 UTF-8 behavior from
RFC 9051.

Required behavior:

- Advertise `UTF8=ACCEPT` only after mailbox parsing, command handling,
  serialization, validation, and tests comply with this document.
- `ENABLE UTF8=ACCEPT` succeeds only when the session can accept UTF-8 mailbox
  names consistently.
- `CREATE`, `RENAME`, `DELETE`, `SUBSCRIBE`, `UNSUBSCRIBE`, `LIST`, `STATUS`,
  `SELECT`, and `EXAMINE` all use the shared decoded `MailboxPath` model.
- `LIST` matching runs over decoded hierarchy paths and follows RFC 9051:
  canonical patterns select returned mailboxes, `%` is one hierarchy level, and
  `*` is recursive.
- `LIST (SUBSCRIBED)`, `LIST RETURN (SUBSCRIBED)`, and compatibility `LSUB`
  derive from canonical persisted subscription state.
- `LIST` responses include special-use attributes for role mailboxes where
  supported; role detection must not depend on localized display names.
- `INBOX` resolution is case-insensitive and role-bound.
- IMAP must not create protocol-local mailbox names, subscription lists, special
  use state, `Sent`, `Drafts`, or `Outbox`.

## JMAP Contract

JMAP mailbox behavior follows RFC 8621 section 2.

Required behavior:

- `Mailbox.name` is a validated Net-Unicode `MailboxSegment`.
- `Mailbox/get` returns the stored NFC display name.
- `Mailbox/set create` and `Mailbox/set update` validate names through the
  shared policy before persistence.
- `Mailbox/set` rejects canonical-equivalent sibling duplicates.
- `Mailbox/set` rejects parent cycles and cross-account parent references.
- `Mailbox.parentId` is implemented immediately; hierarchy must not be inferred
  from `/` in `name`.
- `Mailbox.role` remains language-neutral and maps to IMAP special-use
  identity. A mailbox has at most one role, and no account has two mailboxes
  with the same role.
- `Mailbox.isSubscribed` is persisted per user and shared with IMAP
  subscription behavior.
- JMAP method errors for invalid names should be deterministic. Use the closest
  standard SetError type and include a short description suitable for clients to
  turn into a localized error.

## Storage and Migration Requirements

Implementation must preserve existing ASCII mailbox behavior while adding
Unicode policy:

- Store `display_name_nfc`.
- Store a canonical equality key for sibling uniqueness.
- Store a UTS #39 security skeleton or equivalent indexed comparison artifact
  if needed for efficient strict confusable checks.
- Enforce uniqueness by parent, account-visible namespace, and canonical
  equality key.
- Enforce one role mailbox per account for JMAP compatibility.
- Persist subscription state per user and mailbox.
- Keep role identity separate from display name.
- Keep mailbox ids opaque and stable across renames.
- Never use localized display strings as durable role identifiers.

Existing mailbox rows must be migrated through the same policy before the
feature is advertised. If an existing deployment contains names that would fail
strict validation, migration must stop with a report instead of silently
renaming or deleting mailboxes.

## Implementation Verification

No code is changed by this document, but future implementation is not complete
until these checks exist:

- shared policy tests for NFC normalization and canonical-equivalence
  collision,
- shared policy tests for reserved-name and special-use spoof rejection,
- shared policy tests for controls, default-ignorable characters, bidi controls,
  mixed-script confusables, and whole-script confusables,
- IMAP command tests for `CREATE`, `LIST`, `STATUS`, `SELECT`, `RENAME`,
  `DELETE`, `SUBSCRIBE`, `UNSUBSCRIBE`, and `UTF8=ACCEPT`,
- IMAP `LIST` wildcard tests for Unicode hierarchy segments,
- JMAP `Mailbox/set` tests for Unicode creation, rename, parent validation,
  cycle rejection, duplicate rejection, and deterministic errors,
- JMAP `Mailbox/get` tests proving names are returned in NFC,
- cross-protocol tests proving IMAP subscriptions and JMAP `isSubscribed` share
  the same persisted state,
- real-client transcript coverage before advertising the feature as supported.

Required fixtures include `Café`, `Cafe\u0301`, `案件`, `📁 Projects`, one Arabic
or Hebrew single-script mailbox name, a nested Unicode IMAP path, invalid
control-character names, invalid delimiter-in-segment names, and confusable
spoof attempts.

Current regression examples:

| Area | Tested examples |
| --- | --- |
| Shared mailbox-name policy | `Café`, `Cafe\u0301`, `案件`, `📁 Projects`, `مشاريع`, `משימות`, control characters, zero-width invisible characters, U+202E RIGHT-TO-LEFT OVERRIDE, mixed Latin/Cyrillic `pаypаl`, and whole-script confusable skeleton collision for Cyrillic `раураӏ` versus Latin `paypal`. |
| IMAP workflow | Transcript-style `ENABLE UTF8=ACCEPT`, `CREATE`, `LIST`, `STATUS`, `SELECT`, `RENAME`, and `DELETE` coverage for `📁 Projects`, `案件`, `مشاريع`, `Café`, and nested paths such as `Projects/Alpha` and `案件/顧客`; wildcard hierarchy matching for Unicode paths; canonical parent creation for missing intermediate segments; canonical-equivalent duplicate rejection for `Cafe\u0301` versus `Café`; confusable sibling rejection for Cyrillic `раураӏ` versus Latin `paypal`. |
| JMAP workflow | `Mailbox/set` create/update normalization for `Cafe\u0301` to `Café`, accepted Unicode names including `案件`, `📁 Projects`, and `مشاريع`, deterministic rejection of controls, `/`, invisible and bidi controls, mixed-script spoof strings, reserved names, canonical-equivalent sibling duplicates, and confusable sibling names; `Mailbox/get` returns stored NFC names. |

## Implementation Link

The shared mailbox-name primitives live in
`crates/lpe-domain/src/mailbox_name.rs`. IMAP path handling, JMAP mailbox
create/update parsing, storage duplicate checks, and Sieve `fileinto` mailbox
creation must use that module instead of protocol-local display-name
normalization. IMAP `CREATE` and `RENAME` store nested paths as canonical
mailbox hierarchy rows with `parent_mailbox_id`; stored `display_name` remains
one validated segment, and IMAP renders full paths by joining the parent chain
with `/`.

## Non-Goals

- This document does not define EAI local-part or IDNA mailbox-address policy.
- This document does not change SMTP, delivery, or `LPE-CT` behavior.
- This document does not add localized default folder creation.
- This document does not enable IMAP `CREATE-SPECIAL-USE`.
- This document does not add new dependencies; future dependency choices still
  follow `LICENSE.md`.
