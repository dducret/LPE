# EWS P0/P1/P2 Implementation Prompts

This worklist reflects the current `0.5.3` EWS contract rather than the
historical P0/P1 prompt backlog. It avoids reopening completed bounded work.

## Priority status

| Priority | Worklist status | Prompt use |
| --- | --- | --- |
| P0 | All 23 rows are `Partial`; none is `Missing` or explicitly unsupported. | Active implementation and hardening work. |
| P1 | All 24 rows are `Partial`; none is `Missing` or explicitly unsupported. | Active implementation and hardening work. |
| P2 | `ArchiveItem` is missing; the remaining P2 rows are bounded partial compatibility surfaces. The contract's two persona rows are stale against the implemented matrix and tests. | Active, deliberately scoped follow-up work. |

`Partial` means the current implementation is bounded and differs from full
Exchange semantics. It is not a completion marker. Each P0/P1 prompt must
first demonstrate a concrete remaining behavior gap, then implement and verify
the smallest canonical fix before its operation can move to `Implemented`.

## Shared preamble

Prepend this to every implementation prompt below:

```text
Work in C:\Development\LPE. Before editing, read AGENTS.md, ARCHITECTURE.md,
docs/architecture/initial-architecture.md, LICENSE.md, and
docs/architecture/ews-operation-contract.md, plus only the architecture and
implementation documents directly relevant to this slice. State assumptions,
the smallest plan, and concrete success criteria before changing code.

EWS remains a bounded adapter over canonical LPE state. Do not add an
Exchange-specific store, a protocol-local Outbox or Sent, or client SMTP.
Protect Bcc from normal user-facing, search, AI, and sync projections. Keep
tenant, account, delegation, and public-folder ACL checks before every
projection or mutation. Validate a whole supported request before writing;
rejections must be parseable EWS errors with no partial mutation.

For a Microsoft wire-semantic question not answered by project documentation,
first inspect docs/microsoft/protocol-sources.toml, then use only official
Microsoft Learn Open Specifications and follow AGENTS.md's cache/source-record
procedure. Do not copy external implementation code or add a dependency
without the LICENSE.md review.

This database is test-only. If a schema correction is genuinely required,
edit the canonical fresh schema directly; do not write an upgrade script. Use
the supplied TEST_DATABASE_URL through the environment without printing
credentials. Prefer existing canonical APIs/storage to ad-hoc EWS SQL. Do not
build or deploy a release binary.

Write focused regression tests before each demonstrated fix. Run focused cargo
tests one filter at a time, then cargo test-lpe-exchange-ews. Run matching
schema/storage contract tests if the schema changes. Update architecture
documentation only when behavior or schema facts actually change.
```

## P0 — implementation prompt

```text
Implement the P0 EWS operations in priority order. Apply the shared preamble.
For each operation, first reproduce a concrete gap against its contract, then
add the smallest canonical implementation and focused regression test. Cover
mail items, folders, attachments, identity, availability, Inbox rules,
notifications, and synchronization without broadening into an Exchange store
or losing Bcc, authorization, or transaction guarantees. Do not mark an
operation completed because it has a dispatcher branch or a happy-path test.
```

## P1 — implementation prompt

```text
Implement the P1 EWS operations in priority order. Apply the shared preamble.
For each operation, first reproduce a concrete gap against its contract, then
add the smallest canonical implementation and focused regression test. Cover
bounded item/folder batches, reminders, conversations, IDs, rooms, OOF,
MailTips, delegation, streaming, and user configuration without adding
protocol-local durable state or loosening ACL, cursor, or transaction rules.
Do not mark an operation completed because it has a dispatcher branch or a
happy-path test.
```

## P2 — active prompts

### 1. Reconcile implemented persona documentation

```text
Reconcile the P2 FindPeople and GetPersona rows in
docs/architecture/ews-operation-contract.md with the already implemented
bounded behavior. Apply the shared preamble, but this is documentation-only
unless the evidence exposes a real implementation defect.

The interoperability matrix, EWS dispatcher, and focused tests already show
stateless visible account/contact personas. Verify the exact ID forms,
visibility behavior, and unsupported linked-person scope. Update only the
stale contract rows and, if necessary, their direct cross-references. Do not
add a persona table, linked-contact aggregation, social sources, or Exchange
persona state. Run the EWS catalog gate and git diff --check.
```

### 2. Archive and folder-tree lifecycle

```text
Implement the next bounded P2 folder lifecycle slice: ArchiveItem,
CreateFolderPath, CopyFolder, and MoveFolder. Apply the shared preamble.

First establish whether each operation can map to existing canonical mailbox
and public-folder transactions without an Exchange archive store. ArchiveItem
may only move supported canonical messages to an existing configured Archive
mailbox; it must not invent Exchange online-archive or retention semantics.
Path, copy, and move behavior must completely preflight source, destination,
ACL, system-folder, and recursion constraints, then use one canonical
transaction for the supported scope. Reject unsupported public-folder
reparenting or tree shapes until canonical semantics exist.

Add focused success, ACL, late-invalid-input rollback, hierarchy-sync, and
cross-protocol visibility tests. Update the contract and matrix only for the
actually delivered bounded subset.
```

### 3. Directory and conversation follow-up

```text
Harden the remaining bounded P2 directory and conversation behavior for
ExpandDL and ApplyConversationAction. Apply the shared preamble.

ExpandDL must remain a same-tenant, visibility-checked projection of canonical
groups; add recursive expansion only when cycle detection, limits, and error
semantics are defined through canonical directory state. ApplyConversationAction
may support a persistent Always* action only if the required lifecycle state is
first modeled canonically. Do not stretch mailbox_messages.thread_id into a
thread store; if persistent lifecycle is truly required, document and add a
real threads model before exposing it.

Test tenant isolation, hidden members, cycle/limit rejection where applicable,
and no mutation for unsupported persistent conversation actions. Preserve
parseable gaps when the canonical model is absent.
```

### 4. Junk classification and LPE-CT feedback boundary

```text
Harden P2 MarkAsJunk without moving perimeter filtering into core LPE. Apply
the shared preamble and read the directly relevant LPE-CT and mail-security
architecture documents.

Keep the canonical mailbox move to Junk as the user-visible action. If spam or
not-junk feedback is added, define one authenticated LPE-to-LPE-CT handoff and
an auditable canonical feedback record; never create core SMTP filtering,
blocked/safe sender lists, or Exchange-only junk state. Fully preflight target
visibility and feedback eligibility before mutation, and keep the operation
idempotent across retry.

Test mailbox movement, permission denial, idempotency, handoff failure without
split canonical state, and the strict LPE/LPE-CT boundary. Update architecture
and installation documentation with any real interface change.
```

### 5. Bounded transfer jobs

```text
Extend P2 UploadItems and ExportItems only through canonical transfer jobs.
Apply the shared preamble and read docs/architecture/attachments-v1.md.

Import must retain a bounded job/entry record, validate every client-provided
file with Magika before normal processing, and commit canonical mailbox
membership, blobs, MIME, and change evidence atomically for a supported entry.
Export must reconstruct messages from canonical blobs and metadata without
creating an EWS-only archive or exposing Bcc. Do not implement Exchange
streaming transfer payloads, broad MIME conversion, or an unbounded job queue
unless a canonical product requirement is documented first.

Test job ownership, retry/idempotency, failed-entry isolation, blob
deduplication, Bcc exclusion, export reconstruction, and tenant boundaries.
```

### 6. Retention and service configuration

```text
Harden P2 GetUserRetentionPolicyTags and GetServiceConfiguration against their
canonical sources. Apply the shared preamble and read the directly relevant
data-lifecycle and administration architecture documents.

Retention tags may expose only active same-tenant visible tags plus the
authenticated account's assigned default tag, including a hidden assigned tag
when documented. Service configuration may expose only already implemented
MailTips capability. Return parseable gaps for Unified Messaging, protection
rules, policy tips, and every Exchange-only configuration unless a canonical
LPE model and ownership are approved first.

Test tenant/assignment visibility, hidden-assigned behavior, disabled tags,
unknown configuration requests, and response-shape stability. Do not build an
Exchange managed-folder engine or a parallel policy store.
```

### 7. Canonical same-tenant sharing

```text
Harden P2 sharing compatibility for AcceptSharingInvitation, GetSharingFolder,
and GetSharingMetadata. Apply the shared preamble and read the collaboration
ACL/delegation architecture document.

Use only canonical contact-book and calendar grants. Accept an invitation only
for a same-tenant supported grant; retrieve only an accessible shared
contact/calendar collection; and return metadata only for collections owned by
the authenticated account. Reject mailbox sharing, ungranted or cross-tenant
access, federation, token stores, and Exchange invitation persistence until
canonical sharing state exists.

Test grant create/update idempotency, owner/delegate visibility, revocation,
tenant isolation, response redaction, and no partial grants on malformed input.
```

## P2 coverage

| Prompt | Contract rows |
| --- | --- |
| 1 | `FindPeople`, `GetPersona` documentation drift only; current implementation is already bounded partial support. |
| 2 | `ArchiveItem`, `CreateFolderPath`, `CopyFolder`, `MoveFolder` |
| 3 | `ExpandDL`, `ApplyConversationAction` |
| 4 | `MarkAsJunk` |
| 5 | `UploadItems`, `ExportItems` |
| 6 | `GetUserRetentionPolicyTags`, `GetServiceConfiguration` |
| 7 | `CreateItem` with `AcceptSharingInvitation`, `GetSharingFolder`, `GetSharingMetadata` |

Run P2 prompt 1 first. The others are independent only after their shared
canonical storage or architecture preconditions have been checked; do not run
two prompts that edit the same EWS dispatcher or schema area concurrently.
