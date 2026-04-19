# Mail Security and Traceability | Securite mail et tracabilite

## Francais

### Objectif

Ce document decrit le flux mail, la securite edge, la tracabilite et la quarantaine entre `LPE-CT` et `LPE`.

### Principe

`LPE-CT` recoit, filtre, trace, route et met en quarantaine.

`LPE` persiste les mailbox et reste systeme de record.

La mise en oeuvre actuelle de `LPE-CT` execute deja des validations reelles `SPF`, `DKIM` et `DMARC`, le greylisting, des lookups `DNSBL/RBL`, une reputation locale simple et une trace detaillee de decision persistee dans le spool.

Les decisions edge reposent maintenant sur des verdicts structures:

- `DMARC reject` peut forcer un rejet `SMTP`
- `DMARC quarantine` peut forcer une quarantaine
- `SPF fail` peut forcer un rejet s'il n'existe pas de `DKIM` aligne compensatoire
- un echec temporaire d'authentification (`SPF`/`DKIM`/`DMARC`) peut forcer un `defer`
- une mauvaise reputation expediteur/IP peut forcer une quarantaine ou un rejet

### Scores separes

Le modele doit separer:

- `Spam Score`, probabiliste
- `Security Score`, plus deterministe et oriente risque

### Validation des fichiers par Magika

Tout fichier entrant via connexion externe ou via un client est valide par `Magika` avant traitement normal.

Cela couvre notamment:

- pieces jointes entrantes
- blobs `JMAP`
- imports `PST`
- futurs uploads navigateur ou API

Si `Magika` identifie un fichier exotique, suspect ou interdit par policy, l'action par defaut est la quarantaine dans `LPE-CT`.

Une sandbox d'analyse dynamique pourra exister plus tard, mais elle ne fait pas partie de la base actuelle.

### Messages chiffrés non inspectables

Si un message est chiffre de bout en bout, par exemple `PGP` ou `S/MIME`, et qu'il ne peut pas etre inspecte, il doit etre marque `uninspectable`.

### Magika hors thread SMTP

A terme, `Magika` doit pouvoir tourner hors du thread critique de reception `SMTP`, par exemple dans un worker separe ou un sidecar.

### Propagation des policies

La propagation des policies suit un modele push de `LPE` vers `LPE-CT`.

### Identite de trace unique

Chaque message traite recoit un identifiant de trace unique qui doit survivre jusqu'au statut final.

### Retour de statut final et DSN

Si `LPE` rejette une livraison finale apres acceptation edge, il doit renvoyer ce statut a `LPE-CT`.

`LPE-CT` doit alors pouvoir:

- correler l'erreur avec l'identifiant initial
- garder une trace coherente de bout en bout
- generer un bounce ou `DSN` coherent

La trace persistee doit aussi rester suffisamment structuree pour preparer plus tard:

- `ARC`
- `MTA-STS`
- `TLS-RPT`

### Streaming interne

Quand la livraison peut s'effectuer normalement, l'echange interne `LPE-CT -> LPE` doit supporter le streaming afin d'eviter les doubles ecritures disque inutiles.

### Quarantaine

La quarantaine est stockee dans `LPE-CT`.

`LPE` peut demander la liberation d'un message via une action privilegiee, mais la possession de la quarantaine reste dans le centre de tri.

## English

### Goal

This document describes mail flow, edge security, traceability, and quarantine between `LPE-CT` and `LPE`.

### Principle

`LPE-CT` receives, filters, traces, routes, and quarantines.

`LPE` persists mailboxes and remains the system of record.

The current `LPE-CT` implementation already executes real `SPF`, `DKIM`, and `DMARC` validation, greylisting, `DNSBL/RBL` lookups, simple local reputation, and a detailed decision trace persisted in the spool.

Edge decisions now rely on structured outcomes:

- `DMARC reject` can force SMTP reject
- `DMARC quarantine` can force quarantine
- `SPF fail` can force reject when no aligned `DKIM` pass compensates
- temporary authentication failure (`SPF`/`DKIM`/`DMARC`) can force `defer`
- poor sender/IP reputation can force quarantine or reject

### Separate scores

The model should separate:

- `Spam Score`, probabilistic
- `Security Score`, more deterministic and risk-oriented

### File validation with Magika

Every file entering through an external connection or through a client is validated by `Magika` before normal processing.

This includes in particular:

- inbound attachments
- `JMAP` blobs
- `PST` imports
- future browser or API uploads

If `Magika` identifies an exotic, suspicious, or policy-disallowed file, the default action is quarantine in `LPE-CT`.

A dynamic-analysis sandbox may exist later, but it is not part of the current baseline.

### Encrypted uninspectable messages

If a message is end-to-end encrypted, for example with `PGP` or `S/MIME`, and cannot be inspected, it must be marked `uninspectable`.

### Magika outside the SMTP thread

In the future, `Magika` should be able to run outside the critical `SMTP` receive thread, for example in a separate worker or sidecar.

### Policy propagation

Policy propagation follows a push model from `LPE` to `LPE-CT`.

### Unique trace identity

Each processed message receives a unique trace identifier that must survive until the final outcome.

### Final status return and DSN

If `LPE` rejects final delivery after edge acceptance, it must return that status to `LPE-CT`.

`LPE-CT` must then be able to:

- correlate the error with the original identifier
- keep end-to-end trace search coherent
- generate a consistent bounce or `DSN`

The persisted trace must also stay structured enough to prepare later work on:

- `ARC`
- `MTA-STS`
- `TLS-RPT`

### Internal streaming

When delivery can proceed normally, the internal `LPE-CT -> LPE` exchange should support streaming to avoid unnecessary double disk writes.

### Quarantine

Quarantine is stored in `LPE-CT`.

`LPE` may request the release of a message through a privileged action, but quarantine ownership remains in the sorting center.
