# ActiveSync Adapter

## Current State/Functionality Overview

`lpe-activesync` exposes Exchange ActiveSync as a compatibility adapter for mobile and native clients that support `Exchange ActiveSync`. It reuses canonical `LPE` authentication, mailbox state, draft persistence, contacts, calendar, and submission.

## Implementation/Usage

- Endpoint:
  - `OPTIONS /Microsoft-Server-ActiveSync`
  - `POST /Microsoft-Server-ActiveSync`
- Authentication:
  - mailbox-account `Basic`
  - mailbox bearer tokens from `/api/mail/auth/oauth/access-token`
- Unauthenticated `OPTIONS` requests return a `401` ActiveSync authentication
  challenge with the same protocol capability headers used by successful
  authenticated probes.
- Protocol behavior:
  - parses only the `WBXML` code pages required by the supported command set
  - uses canonical mailbox, contact, and calendar data
  - creates authoritative `Sent` copies through canonical submission
  - hands outbound relay to `LPE-CT`
  - never implements client `SMTP`
  - never creates protocol-specific `Sent`, `Drafts`, or `Outbox` state
- `Sync` behavior:
  - supports multiple collections in one request
  - accepts common protocol options that are not fully material to the adapter response
  - uses persisted sync keys and canonical collection state
  - supports shared mailbox projection for canonical mail folders
- `FolderSync` behavior:
  - projects canonical mailbox folders, including `Inbox`, `Sent`, `Drafts`,
    `Trash`, `Junk`, `Archive`, and user-created mail folders when present
  - preserves canonical mailbox parent-child relationships with ActiveSync
    `ParentId` values instead of flattening mail folders under the root
  - maps unsupported or non-default mail roles to the ActiveSync user-created
    mail folder type instead of inventing ActiveSync-only folder classes
- `SendMail`, `SmartReply`, and `SmartForward`:
  - parse submitted `MIME`
  - validate attachments through the canonical file-validation path
  - create the authoritative `Sent` message before outbound relay
- Contacts and calendar:
  - use canonical `contacts` and `calendar_events`
  - support basic client-originated mutations through `Sync`
- Tasks:
  - are not exposed as an ActiveSync class
  - must reuse the canonical `tasks` model when implemented

## Reference Table/List

| Area | Current support |
| --- | --- |
| Commands | `OPTIONS`, `Provision`, `FolderSync`, `Sync`, `SendMail`, `SmartReply`, `SmartForward`, `ItemOperations Fetch`, `Search`, `Ping` |
| Mail folders | canonical mailbox folders |
| Contacts | canonical contacts projection and basic mutations |
| Calendar | canonical events projection and basic mutations |
| Attachments | common MIME attachment parsing and canonical retrieval |
| Long poll | lightweight `Ping` delta detection against persisted sync state |
| Unsupported | full Exchange server semantics, client `SMTP`, ActiveSync task class, non-canonical outbound logic |
