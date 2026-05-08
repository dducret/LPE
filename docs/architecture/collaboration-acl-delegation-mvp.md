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
  - supports `send-as` and `send-on-behalf` checks during canonical submission
  - does not create protocol-specific submitted-message logic
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

## Reference Table/List

| Permission | Meaning |
| --- | --- |
| `read` | view resource content |
| `write` | create or update resource content |
| `delete` | delete resource content |
| `share` | manage grants where owner policy permits |
| `send-as` | submit with delegated mailbox in `From` |
| `send-on-behalf` | submit with delegated mailbox represented by `Sender` |

| Resource | Canonical source |
| --- | --- |
| Contacts | `contacts` |
| Calendars | `calendar_events` |
| Tasks | `tasks`, `task_lists`, `task_list_grants` |
| Mailboxes | canonical mailbox tables and mailbox delegation grants |
