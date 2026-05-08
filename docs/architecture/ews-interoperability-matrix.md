# EWS Interoperability Matrix

## Current State/Functionality Overview

The EWS matrix defines the live checks required for the supported `lpe-exchange` EWS surface. It validates canonical mailbox, contacts, calendar, task, and submission behavior through `/EWS/Exchange.asmx`.

## Implementation/Usage

- Use the live smoke harness against the public or local EWS URL.
- Require authentication.
- Require canonical `Sent` visibility after EWS send.
- Require contact and calendar create-read-delete checks.
- Require task checks where EWS task operations are published.
- Keep `MAPI/EMSMDB`, `MAPI/NSPI`, and `/rpc/rpcproxy.dll` checks in the Outlook/MAPI readiness path.

## Reference Table/List

| Check | Requirement |
| --- | --- |
| EWS endpoint | `https://mail.example.test/EWS/Exchange.asmx` |
| Smoke script | `tools/ews_live_smoke_check.py` |
| RCA script | `tools/rca_outlook_connectivity_check.py` |
| Mail | find, get, create/send, delete where supported |
| Contacts | create, read, update, delete |
| Calendar | create, read, update, delete, busy status |
| Tasks | create, read, update, delete where supported |
| Submission | canonical `Sent` copy visible after send |
