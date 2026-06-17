# Microsoft Protocol Reference Index

This directory tracks the Microsoft protocol documentation used for LPE protocol
work.

Do not commit Microsoft PDF, DOC, DOCX, HTML exports, or extracted full-text
copies here unless redistribution has been explicitly reviewed and approved.
Keep local copies in `docs/microsoft/cache/`, which is ignored by git.

## Purpose

Use this directory to make protocol work reproducible without copying Microsoft
documentation into the Apache-2.0 source tree.

Committed files should record:

- the protocol short name, such as `MS-OXNSPI`
- the official Microsoft Learn page
- the official PDF or DOCX download URL
- the version or publication date checked
- the local file SHA256 when a cached copy was used
- the exact sections relied on by LPE code or tests

Short quotations can be included only when they are necessary and kept brief.
Prefer section references and LPE-specific interpretation notes.

## Local Cache

Local protocol files belong under:

```text
docs/microsoft/cache/
```

Recommended local naming:

```text
docs/microsoft/cache/MS-OXNSPI-20250520.pdf
docs/microsoft/cache/MS-OXOABK-20250520.pdf
```

After downloading a reference file, record its SHA256 in
`protocol-sources.toml`:

```powershell
Get-FileHash docs\microsoft\cache\MS-OXNSPI-20250520.pdf -Algorithm SHA256
```

## Current Focus

The current high-priority Microsoft references are for Outlook desktop
interoperability through MAPI over HTTP:

- NSPI address book hierarchy and name resolution
- EMSMDB, ROP, and MAPI over HTTP transport behavior
- ICS and FastTransfer cached-mode synchronization
- Outlook Autodiscover and profile bootstrap behavior
