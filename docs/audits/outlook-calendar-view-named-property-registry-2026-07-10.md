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

Run 16:34 on deployed build `aa6508e12e61` validated the code-side
canonicalization: Appointment `0x8214` and `0x8223`, and all requested Common
`0x8500..0x85FF` properties, were returned with canonical IDs. It also exposed
two remaining database occupants in the reserved Appointment range. The
218-property startup response returned `0x0000` for
`http://schemas.microsoft.com/outlook/spoofingstamp` and `DRMLicense`, producing
one duplicate returned-ID collision. Outlook again stopped at
`RopSetColumns`/`RopQueryPosition` request `:235`. This is a pre-final-database-
repair validation run, not a successful acceptance run.

Run 16:53 on deployed build `8773a76b` validated the completed database
repair. Its 218-name startup mapping batch had zero unresolved names, zero
duplicate returned IDs, and zero reserved-range collisions; the later
Appointment mappings were canonical (`0x8205`, `0x8208`, and related LIDs).
The Calendar table still stopped after request `:235`, with the same ten
columns and `RopQueryPosition` result `0/1`. This run separates the repaired
named-property defect from the remaining default-view interoperability defect.

Run 18:24 on deployed build `a441481d6dda` restored the folder-local Calendar
NamedView while retaining the repaired named-property registry. Its 550 RR
events contain 275 HTTP 200 responses, 274 zero MAPI response codes, and no ROP
parse error. `PidTagDefaultViewEntryId` is present, the Calendar associated
table returns the NamedView, and the descriptor has no missing table-backed
column. Outlook nevertheless does not open that view and again stops after
request `:236`, with the same ten-column `RopSetColumns` and `RopQueryPosition`
result `0/1`.

Run 19:00 on deployed build `b4403a39599b` tested the legacy shared MID
`0x7fffffffffe90001` while retaining the clean named-property registry. The
result is unchanged: Outlook enumerates the Calendar FAI row, never opens it,
and stops at request `:236` after the same ten-column QueryPosition. The RCA
also reports the expected duplicate MID ownership between Inbox and Calendar.
The shared-MID hypothesis is therefore falsified and the canonical per-folder
Calendar identity is retained.

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

The earlier failing folder-associated Calendar table exposes the three canonical
configuration messages `IPM.Configuration.AvailabilityOptions`,
`IPM.Configuration.Calendar`, and `IPM.Configuration.WorkHours`, but no
`IPM.Microsoft.FolderDesign.NamedView` row. Folder property
`PidTagDefaultViewEntryId` (`0x3616`, `PtypBinary`) is returned as not found.
Run 18:24 additionally exposes a folder-local `Calendar` NamedView and a
DefaultViewEntryId, but uses MID `0x7ffffffe00100001`. Outlook enumerates this
row without opening it. The known-good `logs/LPE_last_202606251705.log` uses MID
`0x7fffffffffe90001`; Outlook opens that exact FID/MID pair at request `:303`,
then selects 62 Calendar columns and issues repeated `RopQueryRows` calls.

The Microsoft
[PidTagDefaultViewEntryId canonical-property definition](https://learn.microsoft.com/en-us/office/client-developer/outlook/mapi/pidtagdefaultviewentryid-canonical-property)
permits the property to be absent when Normal is the initial view, so absence
alone is not a wire-protocol violation. It is nevertheless the first remaining
semantic difference from the known-good Outlook sequence after the
named-property registry is clean. The implemented compatibility contract uses
the equally valid advertised-default-view path observed in that working trace.
The unopened Calendar contents table retains an empty implicit sort; the view
descriptor's sort becomes table state only after an explicit client operation.
The FAI message class and descriptor fields follow [MS-OXOCFG] sections 2.2.6,
2.2.6.1, 2.2.6.1.1, and 2.2.6.2. Opening its advertised FID/MID uses
[MS-OXCMSG] sections 2.2.3.1 and 3.1.5.1.

## Exact protocol inconsistencies

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

The first proven inconsistency was therefore before `RopSetColumns`: the
working path's named-property state and embedded view/table tags agreed, while
run 15:45 returned database IDs that disagreed with the tags selected
immediately afterward. Run 16:53 proves that repairing this inconsistency was
necessary but not sufficient.

After that repair, the Calendar default-view handoff remains the first visible
difference, but neither its presence nor its MID is sufficient. Run 18:24
restored the view with a per-folder MID; run 19:00 repeated the test with the
legacy shared MID. Both stop at the identical point without opening the view.
The working 17:05 trace had cached client view state and therefore does not
establish which server response creates that state for a clean profile.

The working descriptor has seven packets while the current descriptor has nine,
but Outlook does not open the current view and therefore cannot read either
descriptor property. Changing the descriptor before the open would not address
the first observed divergence. The conforming `RopSetColumns` and
`RopQueryPosition` response remains unchanged pending a clean-profile validation
of this combined server state or a known-good Exchange clean-profile trace.

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

Run 16:34 then proved the first repair's conflict scan was too narrow: it had
relocated only IDs needed by an existing canonical Calendar row. A complete
scan of both reserved ranges found another 176 non-Calendar occupants, including
`spoofingstamp` at `0x822C` and `DRMLicense` at `0x822D`. All 176 were
transactionally relocated to unused `0xF000+` IDs. The complete post-repair
reserved-range conflict count is zero.

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
`RopQueryRows` property row. They also cover `PidTagDefaultViewEntryId`, the
folder-associated Calendar NamedView row, descriptor columns and property
types, the distinct folder-local Calendar identity, and the empty initial
Calendar table sort.

Post-deployment acceptance still requires a new clean Outlook profile to show
that Outlook proceeds beyond `RopQueryPosition` to `RopQueryRows` or content
sync, displays the `Test` appointment, and leaves no actionable Calendar/default
view diagnostic in `tools/rca_outlook_trace_summary.py`.

## Run 19:44 terminal QueryRows-origin regression

Run 19:44 on build `7f45cffc` identified the first wire-semantic regression
from the working 2026-06-25 sequence. Both runs query the Calendar associated
table at position 1 of 4 and return the same final three FAI rows. The working
request `:75` returns `Origin=BOOKMARK_END` (`0x02`), while failing request
`:91` returned `Origin=BOOKMARK_CURRENT` (`0x01`) even though the cursor had
advanced to position 4. Outlook consequently issued an extra empty QueryRows
request before continuing to the visible Calendar table.

`[MS-OXCTABL]` section 2.2.2.1.1 defines `BOOKMARK_END` as the position after
the last row, and section 3.2.5.5 requires it when no more rows remain after a
forward `RopQueryRows`. The response field is defined by `[MS-OXCTABL]`
section 2.2.2.5.2 and `[MS-OXCROPS]` section 2.2.5.4.2. The shared table helper
now returns the boundary bookmark whenever a non-empty or empty query window
reaches that boundary. The regression test reproduces the captured Calendar
FAI state without special-casing Calendar or the trace IDs.
