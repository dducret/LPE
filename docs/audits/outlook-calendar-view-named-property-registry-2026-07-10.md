# Outlook Calendar Normal-view contract RCA (2026-07-10)

## Scope and clean-profile reproduction

This audit diagnoses Outlook's `Outlook cannot display this view` error using
the supplied July 10 server logs and replay traces, including the clean-profile
runs at 10:26, 11:27, and 11:51. Profiles `211`, `212`, and `213` have no
configured OST path. Each clean run opens Calendar folder
`0x0000000000100001`, completes `RopSetColumns` and `RopQueryPosition`, and
stops before `RopQueryRows`.

The 11:51 run is the final pre-fix reproduction:

- server log: `logs/LPE_last_202607101151.log`
- replay trace: `logs/outlook-traces/202607101151`
- deployed build: `29098eb1c8d3`
- EMSMDB session: `{49019ABA-D6A5-4B39-8717-3BE6152F4767}`
- Calendar table: request `:229`, handle `0x8a`
- Calendar named-property mappings: requests `:230` through `:241`
- `RopSetColumns` plus `RopQueryPosition`: request `:242`

All 280 HTTP responses are 200, all 279 MAPI response codes are zero, and the
trace has no ROP parse error. This reproduces a server/client semantic contract
failure rather than a transport, framing, or stale-OST failure.

## Decoded table, named properties, and rows

The canonical Calendar contains one event: MID `0x0000000000440001`, subject
`Test`, duration 30 minutes. The final table selects these properties in order:

`0x67480014, 0x674a0014, 0x674d0014, 0x674e0003, 0x001a001f,
0x0037001f, 0x0e070003, 0x0e170003, 0x85780003, 0x85100003`.

The first four are FolderId, MID, InstID, and InstanceNum. The remaining values
are MessageClass (Unicode), Subject (Unicode), MessageFlags (Integer32),
MessageStatus (Integer32), PSETID_Common LID `0x8578` (Integer32), and
PidLidSideEffects/PSETID_Common LID `0x8510` (Integer32).

An exact focused `RopQueryRows` fixture over this table state returns one
standard, unflagged `PropertyRow`. Its ten values occur in exactly the requested
order and types, and the decoder consumes the complete row with no malformed or
trailing value. This follows [MS-OXCROPS] section 2.2.5.4 and [MS-OXCTABL]
section 2.2.2.5; row encoding was checked against [MS-OXCDATA] sections 2.8.1,
2.8.1.1, and 3.2.

The earlier named-property defect was real but was not the final view failure.
The durable `mapi_named_properties` mapping is now authoritative for every
non-PS_MAPI GUID/LID pair, the session cache preserves its registered ID, and
the inverse mapping is stable. This satisfies [MS-OXCPRPT] sections 3.1.4.1,
3.2.5.9, and 3.2.5.10. The 11:51 trace confirms no property ID is reused for a
different named property.

## Folder-associated data

The folder-local Calendar configuration rows for
`IPM.Configuration.AvailabilityOptions`, `IPM.Configuration.Calendar`, and
`IPM.Configuration.WorkHours` are canonical FAI data and remain exposed.

LPE additionally synthesized an
`IPM.Microsoft.FolderDesign.NamedView` named `Calendar`, advertised it through
`PidTagDefaultViewEntryId` (`0x3616`, `PT_BINARY`; property tag `0x36160102`),
and used its version-8 descriptor to seed an
ascending PidLidCommonStart sort. The descriptor itself is well-formed: it has
nine ColumnPackets, valid property types, no group or restriction, and matching
visible-column and sort references. Its format conforms to [MS-OXOCFG]
sections 2.2.6, 2.2.6.1, and 2.2.6.1.1.

## Exact protocol inconsistency

The clean client enumerates the synthetic Calendar alternate view but does not
open it. It then opens a Normal Calendar contents table. LPE nevertheless
applied the unopened alternate descriptor's PidLidCommonStart sort to that
Normal table. The server therefore combined two mutually exclusive states:

1. `PidTagDefaultViewEntryId` and a folder-associated NamedView claimed an
   alternate view existed.
2. The client selected the Normal view and did not open that descriptor.
3. The server silently seeded the Normal table from the unselected descriptor.

Microsoft's
[`PidTagDefaultViewEntryId` guidance](https://learn.microsoft.com/en-us/office/client-developer/outlook/mapi/pidtagdefaultviewentryid-canonical-property)
makes the property optional when the folder starts in the Normal view. Its
[`Opening a view descriptor` guidance](https://learn.microsoft.com/en-us/office/client-developer/outlook/mapi/opening-a-view-descriptor)
requires an advertised alternate view to be opened and read before it is used.
The inconsistency is therefore not in the conforming final
`RopSetColumns`/`RopQueryPosition` response; it is the synthetic alternate-view
advertisement and its implicit application to a Normal table.

The cursor hypothesis was also checked explicitly. Both successful Inbox and
successful Calendar traces return `Numerator=0` before the first row. Under
[MS-OXCTABL] section 2.2.2.8, a zero numerator with denominator one is the valid
initial table position; it must not be changed to one.

## Known-good comparison

The June 25 traces proceed to Calendar `RopQueryRows`, but they use an existing
cached Outlook view state: Outlook opens the Calendar NamedView and later sends
a wide 60/62-column table. They are useful for validating the row projection,
but they are not evidence that a synthetic alternate view is required for a
clean profile. The first semantic difference in a clean profile is that Outlook
does not open the advertised descriptor before creating the Normal contents
table.

Changing Calendar to reuse Inbox's legacy NamedView MID was tested and rejected.
The 11:51 clean profile still stopped before `RopQueryRows`, and the change made
Inbox and Calendar claim the same MID `0x7fffffffffe90001`. Message EntryIDs
carry folder and message identity as defined by [MS-OXCDATA] section 2.2.4.2;
reusing one synthetic message identity across folder-local views is not the
correct repair.

## Minimal fix and regression contract

Calendar now uses the Normal-view fallback:

- no Calendar `PidTagDefaultViewEntryId` alternate-view advertisement
- no synthetic Calendar `IPM.Microsoft.FolderDesign.NamedView` FAI row
- no descriptor-derived initial sort on the Normal Calendar contents table
- no change to real Calendar configuration FAI rows or canonical events
- no change to `RopSetColumns`, `RopQueryPosition`, or `RopQueryRows` framing

Regression tests cover all four boundaries: the folder property is absent, the
associated table does not synthesize a NamedView, the Normal table starts with
an empty implicit sort, and exact captured-column `RopQueryRows` remains a valid
unflagged row. The fix is centralized in the `lpe-exchange` view helpers and
adds no implementation code to `mapi.rs`.

Protocol sections relied upon are [MS-OXCROPS] sections 2.2.5.1, 2.2.5.4,
2.2.5.7, 2.2.8.1, and 2.2.8.2; [MS-OXCTABL] sections 2.2.2.2, 2.2.2.5, and
2.2.2.8; [MS-OXCPRPT] sections 3.1.4.1, 3.2.5.9, and 3.2.5.10;
[MS-OXOCFG] sections 2.2.6, 2.2.6.1, 2.2.6.1.1, and 3.1.4.3;
[MS-OXCDATA] sections 2.2.4.2, 2.8.1, 2.8.1.1, and 3.2; and [MS-OXCMSG]
sections 2.2.3.1 and 3.1.5.1.
