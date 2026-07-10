# Outlook Calendar view and named-property registry RCA (2026-07-10)

## Reproduction boundary

The failure reproduces with clean Outlook profiles that have no OST. The final
pre-fix reproduction is:

- server log: `logs/LPE_last_202607101545.log`
- replay trace: `logs/outlook-traces/202607101545`
- deployed build: `ada331775832`
- Outlook profile: `217`, with no configured OST path
- EMSMDB session: `{9AE5B3F7-45AA-4C04-8587-CB8CFA7C66E3}`
- Calendar contents table: request `:229`, handle `137`
- final request: `:242`, `RopSetColumns` plus `RopQueryPosition`

The replay contains 562 RR events: 281 HTTP 200 responses, 280 zero MAPI
response codes, and no ROP parse error. Outlook stops after the successful
`RopQueryPosition` response and displays `Outlook cannot display this view`.
Runs 13:20, 13:50, and 15:45 also falsify the provisional alternate-view,
WLink-flags, and WLink-identity hypotheses: removing or stabilizing those states
did not move the terminal boundary.

The cursor hypothesis is false. The response numerator is zero and denominator
is one. A working Inbox table and earlier working Calendar captures also begin
at numerator zero. This is the valid initial position described by
[MS-OXCTABL] sections 2.2.2.8, 2.2.2.8.1, and 2.2.2.8.2.

## Table and row contract

The Calendar FID is `0x0000000000100001`. Its one canonical event is MID
`0x0000000000440001`, subject `Test`, duration 30 minutes. The Normal contents
table has no implicit sort or restriction and selects, in order:

`0x67480014, 0x674a0014, 0x674d0014, 0x674e0003, 0x001a001f,
0x0037001f, 0x0e070003, 0x0e170003, 0x85780003, 0x85100003`.

The final two tags are PSETID_Common LID `0x8578` (`PtypInteger32`) and
PidLidSideEffects, PSETID_Common LID `0x8510` (`PtypInteger32`). A focused
`RopQueryRows` fixture over the exact table state returns one unflagged
`StandardPropertyRow`, containing exactly the ten requested values in the
requested order and types, with no trailing or malformed bytes. The table and
row wire contracts conform to [MS-OXCROPS] sections 2.2.5.1, 2.2.5.4, and
2.2.5.7; [MS-OXCTABL] sections 2.2.2.2, 2.2.2.5, and 2.2.2.8; and
[MS-OXCDATA] sections 2.8.1, 2.8.1.1, and 3.2. The conforming
`RopSetColumns`/`RopQueryPosition` response is not changed by this fix.

The folder-associated Calendar table exposes the three canonical configuration
messages `IPM.Configuration.AvailabilityOptions`,
`IPM.Configuration.Calendar`, and `IPM.Configuration.WorkHours`. Outlook
enumerates them but does not open a Calendar view descriptor before opening the
Normal table. Their descriptors, visible columns, property types, and sort
state are therefore not the terminal inconsistency.

## Exact protocol inconsistency

LPE had two simultaneous registries for the same mailbox named properties:

1. The durable `mapi_named_properties` table returned dynamically allocated
   IDs for PSETID_Appointment and PSETID_Common properties.
2. Calendar table/configuration projection used LPE's canonical Calendar
   profile IDs, where PSETID_Appointment LIDs `0x8200..0x82FF` and
   PSETID_Common LIDs `0x8500..0x85FF` use the same 16-bit property ID as the
   LID.

In run 15:45, Outlook's individual `RopGetPropertyIdsFromNames` requests saw,
among others:

| Named property | Returned before repair | Canonical table ID |
|---|---:|---:|
| PidLidBusyStatus, Appointment `0x8205` | `0x9282` | `0x8205` |
| PidLidLocation, Appointment `0x8208` | `0x9283` | `0x8208` |
| PidLidAppointmentColor, Appointment `0x8214` | `0x8020` | `0x8214` |
| PidLidRecurring, Appointment `0x8223` | `0x8021` | `0x8223` |
| PidLidSideEffects, Common `0x8510` | `0x8005` | `0x8510` |
| Common `0x8578` | `0x8013` | `0x8578` |

The inconsistency was bidirectional. IDs such as `0x8214` and `0x8223` were
also occupied in the database by different GUID/LID pairs, while LPE's inverse
Calendar registry decoded those IDs as PSETID_Appointment. The same named
property consequently had two IDs, and a single ID could denote two different
named properties depending on whether the database or implicit Calendar path
was consulted.

This violates the mapping contract, not the ROP frame shape. [MS-OXCPRPT]
section 2.2.12 says `RopGetPropertyIdsFromNames` maps an abstract named property
to a concrete 16-bit property ID. Sections 2.2.12.1 and 2.2.12.2 require one
response entry for each requested `PropertyName`, in the same order.
[MS-OXCPRPT] section 3.1.4.1 states that the client uses the returned registered
ID for property operations; sections 3.2.5.9 and 3.2.5.10 define the inverse
and forward server processing. The `PropertyName` GUID/LID structure is defined
by [MS-OXCDATA] section 2.6. [MS-OXPROPS] sections 2.9, 2.47, 2.159, 2.216,
and 2.299 identify the exact Appointment/Common GUIDs, LIDs, and property types
used above.

The first semantic difference from the earlier working Calendar path is thus
before `RopSetColumns`: the working path's named-property state and embedded
view/table tags agree, while run 15:45 returns database IDs that disagree with
the tags selected immediately afterward. The earlier view and WLink differences
remain useful falsification evidence but are not the root cause.

## Database repair

The repair was applied transactionally to account
`bc737006-4413-49b9-aefc-3cb6e0088492` (`test@l-p-e.ch`):

- 193 PSETID_Appointment `0x8200..0x82FF` and PSETID_Common
  `0x8500..0x85FF` rows were checked; all 193 were mismatched.
- 65 unrelated occupants of the canonical target IDs were relocated to the
  unused dynamic range beginning at `0xF000`.
- all 193 Calendar-family rows were moved to their canonical property IDs.
- the transaction committed with zero canonical mismatches and zero duplicate
  property IDs.

No `mapi_custom_property_values` rows existed for the account. The stored
folder-profile and associated-configuration values did not reference any of
the relocated mappings, so the repair did not orphan property values.
PSETID_Common LIDs `0x8219`, `0x822C`, and `0x822D` remain dynamically assigned:
they overlap the PSETID_Appointment ID family and canonicalizing them to their
LIDs would itself create a cross-GUID collision.

## Code fix and regression contract

The session resolver now treats the two Calendar families as one canonical
forward registry:

- a stale database mapping for an Appointment/Common Calendar property resolves
  to the canonical Calendar property ID;
- a different property cannot occupy an ID reserved by those Calendar families;
- the stale database ID is retained only as a read-side alias so old selected
  tags can be normalized, and is never returned as the forward mapping;
- non-Calendar named properties continue to use their durable mailbox mapping.

The fix is contained in the `lpe-exchange` named-property/session/dispatch
helpers; no implementation code was added to `mapi.rs`. Regression tests cover
the captured `0x8214 -> 0x8020` and `0x8510 -> 0x8005` stale mappings, reserved
Calendar-ID shadowing, stale-tag normalization, and the exact ten-column
`RopQueryRows` property row.

Post-deployment acceptance still requires a new clean Outlook profile to show
that Outlook proceeds beyond `RopQueryPosition` to `RopQueryRows` or content
sync, displays the `Test` appointment, and leaves no actionable Calendar/default
view diagnostic in `tools/rca_outlook_trace_summary.py`.
