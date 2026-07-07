# Outlook Default View Descriptor Fix Journal - 2026-07-04

## Purpose

Track the Calendar default-view descriptor fix and the follow-up pattern that
was later applied to Contacts, Tasks, Notes, and Journal as Outlook traces proved
those folders also need type-specific default-view metadata during startup.

## Current Calendar Evidence

Run: `LPE_last_202607040823.log`

Outlook showed `Outlook cannot display this view` and was later killed. The
server-side trace showed:

- Calendar normal contents table reached `QueryPosition`.
- LPE projected one Calendar event row at that point.
- Outlook did not issue Calendar `QueryRows` after `QueryPosition`.
- Outlook immediately sent large `GetPropertyIdsFromNames` batches on the
  Logon object.
- The Calendar table columns selected by Outlook included:
  - `0x85780003`
  - `0x85100003`
- The Calendar default view descriptor advertised only:
  - `PidTagMessageClass`
  - `PidTagSubject`
  - `PidLidCommonStart`
  - `PidLidCommonEnd`
  - `PidLidLocation`
  - `PidLidBusyStatus`

The concrete mismatch was that Outlook selected the two common Outlook Calendar
properties after resolving them, but the advertised Calendar descriptor did not
include them.

## Applied Calendar Fix

Files:

- `crates/lpe-exchange/src/mapi/properties/views.rs`
- `crates/lpe-exchange/src/mapi/dispatch/tests.rs`
- `crates/lpe-exchange/src/mapi/properties/tests.rs`

Change:

- Add `PidLidOutlookCommon8578` / `0x85780003` to the Calendar default view
  descriptor.
- Add `PidLidSideEffects` / `0x85100003` to the Calendar default view
  descriptor.
- Update descriptor and diagnostic tests to assert the expanded Calendar
  descriptor.

Validation run before the next Outlook test:

- `rustfmt --edition 2021 --check crates/lpe-exchange/src/mapi/properties/views.rs crates/lpe-exchange/src/mapi/dispatch/tests.rs crates/lpe-exchange/src/mapi/properties/tests.rs`
- `cargo test -p lpe-exchange folder_default_view_definitions_use_type_specific_columns`
- `cargo test -p lpe-exchange outlook_view_descriptor_visible_property_tags_reports_calendar_columns`
- `cargo test -p lpe-exchange calendar_view_handoff_table_contract_reports_calendar_default_view`

## Current Advertising Policy

Calendar, Contacts, Tasks, Notes, and Journal now advertise
`PidTagDefaultViewEntryId` with folder-local, type-specific default view
descriptors. The earlier investigation gate is closed because Calendar evidence
and later Outlook traces proved the default-view path is required for these
folder classes.

Do not blindly expand any descriptor beyond the backed properties already
advertised for that folder type. For each future descriptor change, first confirm
the same failure shape in a real Outlook run:

1. The folder's normal contents table reaches `QueryPosition`.
2. LPE can project at least one row for the folder.
3. Outlook stops before `QueryRows`.
4. The next Outlook activity is named-property resolution or view setup.
5. The selected table columns include backed properties that the default view
   descriptor does not advertise.

Only then align the descriptor with the Outlook-selected backed columns and add
focused tests. The decision point is no longer default-view publication for
Tasks, Notes, or Journal; they should publish one. The remaining decision is
whether their existing descriptor columns are complete for the latest trace.

## Folder Follow-Up Checklist

Contacts:

- Compare `outlook_contact_view_definition` columns with the selected Contacts
  normal contents-table columns in the next failing run.
- Check for selected backed contact properties missing from the descriptor,
  especially email alias, phone, company, title, and Outlook contact-source
  named properties already mapped in the named-property registry.
- Add or update descriptor tests only for properties proven by traces.

Tasks:

- Compare `outlook_task_view_definition` with Outlook-selected task table
  columns.
- Watch for `PidLidTaskDueDate`, `PidLidTaskStartDate`,
  `PidLidPercentComplete`, and any selected task status/date properties that
  are backed by task row synthesis but missing from the descriptor.
- Keep task-specific properties under the Task property set.

Notes:

- Compare `outlook_note_view_definition` with Outlook-selected notes table
  columns.
- Watch for `PidLidNoteColor`, note geometry properties, and message-class /
  subject columns.
- Do not add placeholder note columns unless the row projection returns real
  values or intentional protocol defaults.

Journal:

- Compare `outlook_journal_view_definition` with Outlook-selected journal table
  columns.
- Watch for `PidLidLogStart`, `PidLidLogDuration`, `PidLidLogType`, and
  `PidLidLogTypeDesc`.
- Confirm Journal row projection returns matching values before expanding the
  descriptor.

## Required Evidence For Each Follow-Up Fix

For each folder type, record:

- log filename and Outlook user-visible error text
- EMSMDB session id and request id for the folder `QueryPosition`
- selected table columns
- advertised descriptor columns before the fix
- row projection summary
- named-property burst summary after `QueryPosition`
- exact descriptor columns added
- focused test names
- next Outlook run result

## Stop Conditions

Do not apply this descriptor pattern if Outlook sends `QueryRows` and then fails
later. That is a different bug path and should be diagnosed from the subsequent
ROP, row serialization, or object-open behavior.

Do not add descriptor columns for properties that are unresolved, unbacked, or
only guessed from adjacent Outlook versions. Use real Outlook traces and LPE
row projection evidence first.
