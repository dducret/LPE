# Collaboration ACL and Delegation

## Current State/Functionality Overview

`LPE` uses canonical ACL and delegation tables for contacts, calendars, task lists, and mailbox delegation. Protocol adapters must enforce these grants instead of creating protocol-local rights.

## Implementation/Usage

- Canonical collections:
  - contacts
  - calendars
  - task lists
  - mailboxes
- Grant rules:
  - grants are same-tenant only
  - owners retain full control
  - protocol adapters expose only collections visible to the authenticated account
  - revocation removes protocol visibility
  - cross-tenant sharing is not supported
- Mailbox delegation:
  - supports shared mailbox projection
  - supports delegated mailbox read/write where granted
  - `/api/mail/workspace?accountId={mailbox-account-id}` authorizes the target
    account before loading its canonical mailbox state; the workspace projection
    continues to use the authenticated principal's access-aware contact and
    calendar collection visibility
  - accepts bounded Outlook `RopModifyPermissions` folder ACL rows only when they map to same-tenant canonical mailbox delegation grants
  - supports `send-as` and `send-on-behalf` checks during canonical submission
  - does not create protocol-specific submitted-message logic
  - stores the mailbox-level owner/delegate meeting-delivery policy in
    `delegate_preferences`: meeting requests go to the delegate, delegate and
    owner, or owner; the delegate can receive meeting-request copies and may be
    allowed to view private items
  - `/api/mail/delegation` and the private JMAP `Share/get` projection return
    that preference tuple only on mailbox shares; mailbox-delegation `PUT` and
    JMAP `Share/set` update supplied fields and preserve omitted fields
  - mailbox `Share` reads and deletion target only the owner's default Inbox;
    deleting that relation deletes its preference tuple while custom-folder ACLs,
    calendar grants, and sender rights remain independently managed relations
- Delegate / free-busy object layer:
  - `/api/mail/delegation/free-busy` returns canonical delegate access objects
    and computed free/busy blocks for Outlook, EWS, and MAPI consumers
  - free/busy is scoped to the target mailbox owner's default calendar; custom
    shared calendars are visible through calendar collection grants but are not
    merged into this availability response
  - free/busy is computed from canonical `calendar_events`, not stored as an
    Exchange-specific public-folder free/busy store
  - same-tenant users can retrieve availability blocks; users with canonical
    read access to the owner's default Calendar can distinguish tentative from
    busy, while users without that access receive availability-only `busy`
    blocks; access to another custom calendar owned by the same account does not
    reveal default-Calendar status detail
  - cancelled events are omitted from free/busy output
  - delegate meeting-object handling is considered enabled only when the
    grantee has calendar write access and canonical `send-on-behalf`
  - Outlook `LocalFreebusy` is a MAPI Delegate Information projection over this
    canonical state. Its durable MAPI identity/version metadata is not an API or
    JMAP object and `mapi_object_identities` is never published through either
    surface.
  - `delegation_projection_state` is only a monotonic invalidation marker for
    that computed MAPI row, not a delegate/free-busy content store. Its current
    and applied revisions serialize the canonical delegate read with MAPI
    identity rotation. It advances
    for semantic changes to default-Inbox grants, default-Calendar grants,
    account-wide sender rights, and delegate preferences, including the last
    hard delete, and changes to a projected delegate's name or primary email.
    Secondary collection grants, identity-specific sender rights,
    and ordinary calendar-event changes do not advance it.
- JMAP:
  - exposes shared objects through canonical account and collection visibility
  - uses canonical state/change channels
  - must wake affected principal accounts on shared-object changes
- DAV:
  - exposes shared address books at `/dav/addressbooks/me/{collection-id}/`
  - exposes shared calendars at `/dav/calendars/me/{collection-id}/`
  - exposes task lists at `/dav/calendars/me/tasks-{task-list-id}/`
- Audit:
  - record grant create/update/delete actions
  - record delegated submission decisions
  - preserve actor, owner, grantee, resource, and permission set
- Change replay:
  - write object-level change-log rows for collaboration objects and grants
  - write tombstones for deleted contacts, events, task lists, tasks, collaboration grants, mailbox delegation grants, and sender rights
  - revocation wakes affected principals and removes protocol visibility through canonical change state

## Reference Table/List

| Permission | Meaning |
| --- | --- |
| `read` | view resource content |
| `write` | create or update resource content |
| `delete` | delete resource content |
| `share` | manage grants where owner policy permits |
| `send-as` | submit with delegated mailbox in `From` |
| `send-on-behalf` | submit with delegated mailbox represented by `Sender` |
| `free-busy` | view computed availability without exposing event details |

| Resource | Canonical source |
| --- | --- |
| Contacts | `contacts` |
| Calendars | `calendar_events` |
| Tasks | `tasks`, `task_lists`, `task_list_grants` |
| Mailboxes | canonical mailbox tables and mailbox delegation grants |
| Delegate preferences | `delegate_preferences` attached to a mailbox owner/delegate relation |
| Delegate/free-busy objects | `calendar_grants`, `sender_rights`, `delegate_preferences`, `calendar_events`; `delegation_projection_state` is version metadata only |
