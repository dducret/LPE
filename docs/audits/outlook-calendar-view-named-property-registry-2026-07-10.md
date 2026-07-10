# Outlook Calendar default-view contract RCA (2026-07-10)

## Scope and reproduction

This audit diagnoses the Outlook Calendar error `Outlook cannot display this
view` from:

- `logs/outlook-traces/2026070101026`
- `logs/LPE_last_202607101026.log`
- `logs/outlook-traces/202607101127`
- `logs/LPE_last_202607101127.log`
- account `test@l-p-e.ch` in the supplied PostgreSQL test database

The failing run used Outlook profile `211`. Its startup export is
`C:\Users\dedu\AppData\Local\Temp\Outlook Logging\Prof_001_OUTLOOK_80e8_OutlookStart_2026.07.10_08.25.13.txt`.
That profile has no configured OST path and no account OST file. The trace
starts immediately after that startup, so the failure is reproduced without a
stale OST or a previously cached Calendar view.

The transport and ROP layers are clean: all 566 replay/RR events pair, all
authenticated HTTP responses are 200, all 282 MAPI response codes are zero,
and there are no ROP parse errors. Session
`{80AE9E42-E147-447E-8F61-91038E590A0C}` opens Calendar table handle `0x8a`
at request `:231`, receives a successful `RopSetColumns` and
`RopQueryPosition` at request `:244`, and stops before `RopQueryRows`.

## Calendar table and FAI contract

The normal Calendar table is folder `0x0000000000100001`, contains one event
(`MID 0x0000000000440001`, subject `Test`, duration 30 minutes), and starts
with the implicit ascending `PidLidCommonStart` sort. Request `:244` selects,
in order:

`0x67480014, 0x674a0014, 0x674d0014, 0x674e0003, 0x001a001f,
0x0037001f, 0x0e070003, 0x0e170003, 0x85780003, 0x85100003`.

The folder-local associated table exposes the persisted AvailabilityOptions,
Calendar, and WorkHours configuration messages plus the virtual
`IPM.Microsoft.FolderDesign.NamedView` row referenced by
`PidTagDefaultViewEntryId`. The Calendar descriptor is internally consistent:

- version 8, flags 0, nine ColumnPackets, no groups or restriction
- sort-column index 5, ascending `PidLidCommonStart`
- visible columns: MessageClass (Unicode), Subject (Unicode), MessageFlags
  (Integer32), MessageStatus (Integer32), CommonStart (Time), CommonEnd (Time),
  Location (Unicode), and BusyStatus (Integer32)
- all GUID/LID references and types match the selected property definitions

This matches [MS-OXOCFG] sections 2.2.6, 2.2.6.1, and 2.2.6.1.1. The FAI
descriptor and the conforming `RopSetColumns`/`RopQueryPosition` frames are not
changed by this fix. Their wire shapes were checked against [MS-OXCROPS]
sections 2.2.5.1 and 2.2.5.7 and [MS-OXCTABL] sections 2.2.2.2 and 2.2.2.8.

## Exact inconsistency

Requests `:231` through `:243` resolve PSETID_Appointment LIDs `0x820d`,
`0x820e`, `0x8213`, `0x8208`, `0x8216`, `0x8233`, `0x8205`, `0x8214`,
`0x825e`, `0x8223`, `0x8234`, `0x8217`, and `0x8215`. LPE incorrectly returned
each LID as its wire property ID before consulting the durable account mapping.

The database proves that this is not an aliasing preference but property-ID
reuse:

| Named property | Registered property ID | Incorrect returned ID | Existing owner of incorrect ID |
| --- | ---: | ---: | --- |
| PSETID_Common/LID `0x8510` (SideEffects) | `0x8005` | `0x8510` | no matching registration |
| PSETID_Common/LID `0x8578` | `0x8013` | `0x8578` | no matching registration |
| PSETID_Appointment/LID `0x8214` (AppointmentColor) | `0x8020` | `0x8214` | GUID `90dad86e-0b45-1b10-98da-00aa003f1305`, LID `0x001d` |
| PSETID_Appointment/LID `0x8223` (Recurring) | `0x8021` | `0x8223` | GUID `14200600-0000-0000-c000-000000000046`, LID `0x8f01` |

[MS-OXCPRPT] section 3.1.4.1 defines the server-returned registered ID as the
ID the client subsequently uses. Section 3.2.5.10 allows deriving the ID from
the LID only for `PS_MAPI`; every other property set must use the registered
mapping. A newly assigned ID must be greater than `0x8000`, not `0xffff`,
unique, and not assigned to another named property. Section 3.2.5.9 defines the
inverse ID-to-name contract. The observed responses violate both stability and
uniqueness.

## Working-trace comparison

`logs/LPE_last_202606251705.log` is an earlier working Outlook run (Outlook
16.0.20026.20182, LPE commit `44fd7997fbbc`) that proceeds from Calendar
`RopQueryPosition` to repeated `calendar_normal_query_rows` calls and advances
the table to position 1. Its session registry still identifies property ID
`0x8214` as the Meeting-set LID `0x001d` and `0x8223` as GUID
`14200600-0000-0000-c000-000000000046`/LID `0x8f01`; it does not reassign those
IDs to AppointmentColor and Recurring during a fresh per-property mapping
sequence. The July 10 clean profile performs that sequence, and requests
`:238` and `:240` are the first semantic divergence: the same IDs are returned
for different GUID/LID pairs. The later `RopSetColumns`/`RopQueryPosition`
shape remains conforming in both runs.

## Focused QueryRows validation

The regression fixture recreates the exact normal Calendar table, sort, and ten
columns. `RopQueryRows` returns one standard (unflagged) PropertyRow with an
86-byte value block. It contains exactly the requested values in order:
Calendar FID, MID, InstID, InstanceNum, `IPM.Appointment`, `Test`, message flags
1, message status 0, LID `0x8578` value 0, and SideEffects 369. No bytes remain
after decoding the tenth property. This rules out a malformed or flagged row as
the pre-QueryRows failure.

## First correction

The durable `mapi_named_properties` row is now authoritative for wire IDs.
Session caching preserves that registered ID instead of canonicalizing it to a
LID-shaped internal alias, and `RopGetPropertyIdsFromNames` consults the store
for every non-`PS_MAPI` property. LPE's existing table-column normalization
continues to translate registered wire IDs such as `0x8005` and `0x8013` to the
canonical internal SideEffects and `0x8578` projections. Canonical Calendar
state and the FAI descriptor are unchanged.

## Fresh-profile follow-up and final root cause

The named-property correction was deployed as clean build `f314cf1bc75a` and
retested with fresh Outlook profile `212`, again without an OST. The second run
proves that the durable mappings are now stable and collision-free, but Outlook
still stops after Calendar request `:251` (`RopSetColumns` plus
`RopQueryPosition`). This separates the named-property defect from the remaining
view-selection failure.

The first semantic difference from the known-good June 25 trace occurs before
the normal Calendar table is created:

| Contract | Known-good trace | Failing clean profile |
| --- | --- | --- |
| `PidTagDefaultViewEntryId` Calendar MID | `0x7fffffffffe90001` | `0x7ffffffe00100001` |
| Calendar FAI `PidTagMid` | `0x7fffffffffe90001` | `0x7ffffffe00100001` |
| Outlook action | opens the `Calendar` NamedView with `RopOpenMessage` | never opens the NamedView |
| Resulting table | 60/62 Calendar columns, then `RopQueryRows` | 10 bootstrap columns, then stops after `RopQueryPosition` |

The July 7 folder-specific virtual-ID change altered the identity of the
canonical Calendar view. The FAI descriptor remained valid, but Outlook did not
follow the new EntryID to the descriptor and therefore never reached the
working Calendar table state. The known-good trace opens the folder-local
`IPM.Microsoft.FolderDesign.NamedView` with the stable MID before issuing the
wide Calendar `RopSetColumns`, `RopQueryPosition`, and repeated `RopQueryRows`
sequence.

This relies on [MS-OXCDATA] section 2.2.4.2 for the Message EntryID structure
(which carries both the folder and message identity), [MS-OXCMSG] sections 2.2
and 3.1.5.1 for opening the view message by FID/MID, and [MS-OXOCFG] sections
2.2.6 and 3.1.4.3 for the folder-associated view-definition message and client
selection/open behavior.

## Final fix

Calendar once again uses the stable Outlook default-view MID
`0x7fffffffffe90001` and its matching virtual canonical identity. Other
folder-specific default-view identities remain unchanged. The folder property,
associated-table row, and materialized NamedView message now agree on that
identity. The patch changes no `RopSetColumns`, `RopQueryPosition`, or
`RopQueryRows` framing and adds no trace/session-specific branch.
