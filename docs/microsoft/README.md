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
docs/microsoft/cache/MS-OXNSPI.pdf
docs/microsoft/cache/MS-OXOABK.pdf
```

The publication date and SHA-256 are authoritative in
`protocol-sources.toml`; the active cache filename remains stable when an
official revision replaces an older PDF.

After downloading a reference file, record its SHA256 in
`protocol-sources.toml`:

Use `[[protocol]]` for Microsoft protocol IDs that should reconcile with the
generated constants gap report. Use `[[standard]]` for Outlook or Exchange
standards-support documents that supplement a public standard without defining
a separate LPE protocol surface. Use `[[reference]]` only for documentation-set
or roadmap files that support audit context but are not protocol surfaces.

```powershell
Get-FileHash docs\microsoft\cache\MS-OXNSPI.pdf -Algorithm SHA256
```

## Cache Audit

On 2026-07-18, all 131 active `[[protocol]]` PDFs were downloaded again from
their recorded official Microsoft URLs. The manifest paths, internal release
dates, and SHA-256 values were reconciled with those files, and all extracted
text copies under `cache/tmp-text/` were regenerated. Ninety-six protocol
release dates advanced and 107 PDF contents changed relative to the legacy
cache. Every recorded numeric section was then located in the refreshed full
text; section anchors whose semantics moved were updated to their current
numbers and titles.

The same audit found six unregistered Outlook/Exchange standards-support PDFs
from 2016. They were replaced with the current `MS-STANOICAL`, `MS-STANOIMAP`,
`MS-STANOPOP3`, `MS-STANXICAL`, `MS-STANXIMAP`, and `MS-STANXPOP3` publications,
recorded as `[[standard]]` entries, and extracted under `cache/tmp-text/`.

`ReadmefirstExProto` remains a historical 2015 documentation-set reference in
`cache/archive/`; it is not an authoritative current protocol source.

## Current Focus

The current high-priority Microsoft references are for Outlook desktop
interoperability through MAPI over HTTP:

- NSPI address book hierarchy and name resolution
- EMSMDB, ROP, and MAPI over HTTP transport behavior
- ICS and FastTransfer cached-mode synchronization
- Outlook Autodiscover and profile bootstrap behavior
