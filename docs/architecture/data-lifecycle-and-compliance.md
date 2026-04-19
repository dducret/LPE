# Data Lifecycle and Compliance | Cycle de vie des donnees et compliance

## Francais

### Objectif

Ce document couvre le stockage mailbox, l'archivage, la retention, la deduplication, la confidentialite et les traitements asynchrones.

### Tiers de taille mailbox

- moins de `10 GB`: base partagee
- plus de `10 GB`: base dediee
- plus de `50 GB`: partition technique invisible a l'utilisateur
- plus de `100 GB`: archive en ligne a performances degradees

### Import et export `PST`

`LPE` supporte l'import et l'export `PST` complets ou partiels.

### Deduplication

Les blobs identiques sont dedupliques a l'echelle du domaine.

Les exports doivent reconstruire correctement les pieces jointes.

### Retention et legal hold

La retention peut etre definie au niveau:

- tenant
- domaine
- mailbox
- dossier

Le legal hold est pilote au niveau tenant.

### `Bcc`

`Bcc` est une metadonnee protegee.

Elle:

- n'est pas indexee dans la recherche standard
- n'entre pas dans les pipelines IA utilisateur
- reste disponible pour audit/compliance via workflows privilegies

### Suppression et audit

Les messages supprimes doivent etre reconstruits puis deplaces dans un `Audit Store` separe afin de ne pas polluer les index de production.

### Extraction de texte asynchrone

L'extraction de texte des pieces jointes ne doit pas se faire dans le flux synchrone de reception `SMTP`.

## English

### Goal

This document covers mailbox storage, archiving, retention, deduplication, privacy, and asynchronous processing.

### Mailbox size tiers

- below `10 GB`: shared database
- above `10 GB`: dedicated database
- above `50 GB`: user-invisible technical partitioning
- above `100 GB`: online archive with degraded performance

### `PST` import and export

`LPE` supports full or partial `PST` import and export.

### Deduplication

Identical blobs are deduplicated at domain scope.

Export paths must reconstruct attachments correctly.

### Retention and legal hold

Retention may be defined at:

- tenant
- domain
- mailbox
- folder

Legal hold is governed at tenant level.

### `Bcc`

`Bcc` is protected metadata.

It:

- is not indexed in standard search
- is excluded from user-facing AI pipelines
- remains available for audit and compliance through privileged workflows

### Deletion and audit

Deleted messages must be reconstructed and moved into a separated `Audit Store` so they do not pollute production indexes.

### Asynchronous text extraction

Attachment text extraction must not happen in the synchronous `SMTP` receive path.
