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
  - supports both plain ASHTTP query parameters and base64-encoded ASHTTP query values for implemented commands
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
  - returns ActiveSync `Sync` status `3` for unknown, expired, stale, or
    superseded collection sync keys
  - returns ActiveSync `Sync` status `12` when a collection sync key predates a
    folder hierarchy change that the device has not yet acknowledged through
    `FolderSync`
  - applies mail `Change` requests for `email:Read` to canonical mailbox read
    state
  - applies mail `Delete` requests to canonical mailbox state; `DeletesAsMoves`
    is honored by moving the addressed source membership to canonical `Trash`
    when available, while `DeletesAsMoves=0` permanently removes the addressed
    canonical mailbox membership
  - does not accept client-originated mail body, recipient, subject, or
    attachment mutation through non-draft `Sync`; those remain unsupported until
    canonical mailbox edit semantics are documented
  - honors `BodyPreference` for plain text, sanitized HTML when stored, and
    MIME when the canonical raw-message blob is available; unsupported body
    types fall back to plain text
  - supports shared mailbox projection for canonical mail folders
- `FolderSync` behavior:
  - projects canonical mailbox folders, including `Inbox`, `Sent`, `Drafts`,
    `Trash`, `Junk`, `Archive`, and user-created mail folders when present
  - preserves canonical mailbox parent-child relationships with ActiveSync
    `ParentId` values instead of flattening mail folders under the root
  - maps unsupported or non-default mail roles to the ActiveSync user-created
    mail folder type instead of inventing ActiveSync-only folder classes
  - returns ActiveSync `FolderSync` status `9` for unknown, expired, stale, or
    superseded hierarchy sync keys
  - advances the device hierarchy generation used to validate later collection
    `Sync` retries after hierarchy changes
- `Ping` behavior:
  - requires an initial request with `HeartbeatInterval` and at least one
    monitored `Folder` containing both `Id` and `Class`
  - persists the latest valid Ping heartbeat and monitored folder list per
    account/device so later empty Ping requests remain restart-safe
  - uses implementation-specific heartbeat bounds of 60 through 3540 seconds;
    out-of-range requests return ActiveSync `Ping` status `5` with the nearest
    supported `HeartbeatInterval`
  - limits one Ping request to 200 monitored folders and returns status `6`
    with `MaxFolders` when the request exceeds that limit
  - returns status `3` when required Ping parameters are missing and no cached
    value exists, or when a monitored collection has no prior completed `Sync`
    state for the device
  - returns status `1` when the heartbeat decision finds no additions into the
    monitored collections, and status `2` with response `Folders/Folder`
    string values for collections with additions or moves/copies into the
    collection
  - waits up to the bounded `HeartbeatInterval` before returning status `1`;
    canonical mail, contact, and calendar change notifications wake the
    request early so the adapter can re-check canonical collection state
  - returns status `7` when a monitored folder id/class is no longer valid or
    when the device's acknowledged folder hierarchy is stale; clients must run
    `FolderSync` and then reissue `Ping`
  - remains a bounded persisted-state delta check over canonical state; it does
    not add separate ActiveSync push state
- `SendMail`, `SmartReply`, and `SmartForward`:
  - parse submitted `MIME`
  - validate attachments through the canonical file-validation path
  - create the authoritative `Sent` message before outbound relay
- `MoveItems`:
  - moves mail between canonical mail folders for the same accessible mailbox
    account
  - returns ActiveSync `MoveItems` status `3` on success, `1` for invalid source
    item/source collection, `2` for invalid destination collection, and `4`
    when source and destination folders are the same
  - preserves the canonical message identifier as the destination server ID
- `ItemOperations Fetch`:
  - fetches message application data by `CollectionId` and `ServerId`
  - fetches attachment bytes by `FileReference` from canonical attachment blobs
  - returns attachment content inline in WBXML; multipart response streaming is
    not implemented
  - does not implement legacy `GetAttachment`; protocol versions that need
    attachment bytes must use `ItemOperations Fetch`
- Contacts and calendar:
  - use canonical `contacts` and `calendar_events`
  - support client-originated create, update, and delete through `Sync` for
    fields that round-trip through canonical contact and calendar APIs
  - contact `ApplicationData` maps `FileAs`, `FirstName`, `LastName`,
    `Email1Address`, `MobilePhoneNumber`, `BusinessPhoneNumber`,
    `HomePhoneNumber`, `CompanyName`, `JobTitle`, `Title`, and
    `AirSyncBase:Body` notes onto canonical display name, email, phone,
    organization, role, and notes
  - calendar `ApplicationData` maps `UID`, `Subject`, `StartTime`, `EndTime`,
    `TimeZone`, `Location`, `AirSyncBase:Body`, `Attendees`, and simple
    `Recurrence` patterns onto canonical event UID, title, local start,
    duration, time-zone string, location, body, attendee metadata, and
    recurrence rule
  - recurrence support is limited to canonical `RRULE` patterns that can be
    represented without exceptions: daily, weekly, absolute monthly, and
    absolute yearly recurrences with optional interval, count, or until date
  - ActiveSync attendee name, email, required/optional type, and response
    status map to canonical attendee metadata when present
  - unsupported contact fields include contact photos, postal addresses,
    categories, birthdays, anniversaries, children, spouse, assistant, web
    page, and secondary email/phone slots until the canonical contact API
    exposes those fields
  - unsupported calendar fields include binary Windows time-zone conversion,
    all-day events, reminders, busy/sensitivity status, categories, recurrence
    exceptions, deleted occurrences, online meeting links, proposed new times,
    and client-originated organizer changes until canonical event APIs expose
    matching state
- Tasks:
  - are not exposed as an ActiveSync class
  - must reuse the canonical `tasks` model when implemented

## Reference Table/List

| Area | Current support |
| --- | --- |
| Commands | `OPTIONS`, `Provision`, `FolderSync`, `Sync`, `MoveItems`, `SendMail`, `SmartReply`, `SmartForward`, `ItemOperations Fetch`, `Search`, `Ping` |
| Mail folders | canonical mailbox folders |
| Contacts | canonical contacts projection; `Sync` create/update/delete for name, email, phone, organization, title, notes |
| Calendar | canonical events projection; `Sync` create/update/delete for UID, title, start, duration, time-zone string, location, body, attendees, simple recurrence |
| Mail lifecycle | canonical folder moves, delete/move-to-trash, read/unread state |
| Body preferences | text, stored sanitized HTML, canonical MIME blob with truncation |
| Attachments | common MIME attachment parsing and canonical `FileReference` retrieval |
| Long poll | canonical-change-aware `Ping` long polling against persisted sync state |
| Unsupported | full Exchange server semantics, client `SMTP`, ActiveSync task class, non-canonical outbound logic, legacy `GetAttachment`, multipart `ItemOperations` responses, non-draft mail edits through `Sync`, contact photos/postal addresses/categories, binary Windows time-zone conversion, calendar recurrence exceptions/all-day/reminders/busy status/client-originated organizer changes |
