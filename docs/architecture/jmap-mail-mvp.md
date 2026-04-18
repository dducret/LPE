# JMAP Mail MVP | MVP JMAP Mail

## Francais

### Objectif

Ce document decrit le perimetre `JMAP Mail` actuellement supporte par `LPE` pour le MVP.

Le role de `crates/lpe-jmap` est celui d'un adaptateur `JMAP` au-dessus du modele canonique `LPE` deja en place dans `lpe-storage`. Il ne cree aucune logique parallele de `Sent`, `Drafts`, `Outbox` ou de transport `SMTP`.

### Authentification

- le client `JMAP` reutilise l'authentification compte existante
- le login reste `/api/mail/auth/login`
- le bearer token de session compte existant doit ensuite etre presente a `/api/jmap/session` et `/api/jmap/api`
- sans reverse proxy Debian, les memes routes sont exposees en direct sous `/jmap/session` et `/jmap/api`

### Capacites de session supportees

- `urn:ietf:params:jmap:core`
- `urn:ietf:params:jmap:mail`
- `urn:ietf:params:jmap:submission`

La session `JMAP` est reelle: elle est construite a partir du compte authentifie et expose les `accountId` du compte `LPE` courant.

### Methodes supportees

- `Mailbox/get`
- `Mailbox/query`
- `Mailbox/changes`
- `Mailbox/set`
- `Email/query`
- `Email/get`
- `Email/changes`
- `Email/set` pour la creation, mise a jour et suppression de brouillons
- `Email/copy`
- `Email/import`
- `EmailSubmission/get`
- `EmailSubmission/set` pour la soumission d'un brouillon via le modele canonique `LPE`
- `Identity/get`
- `Thread/get`
- `Thread/changes`
- `Quota/get`
- `SearchSnippet/get`

Routes `JMAP` supportees en plus:

- `POST /api/jmap/upload/{accountId}` pour l'upload d'un blob temporaire `JMAP`
- `GET /api/jmap/download/{accountId}/{blobId}/{name}` pour le telechargement du blob temporaire

### Regles MVP importantes

- `Email/set` persiste uniquement dans la mailbox `Drafts`
- `EmailSubmission/set` ne soumet pas directement un MIME ni un flux `SMTP`
- `EmailSubmission/set` prend un `emailId` de brouillon existant et appelle la soumission canonique `LPE`
- la soumission canonique cree la copie autoritative dans `Sent`, place l'etat du message a `queued`, ajoute l'entree `outbound_message_queue`, puis supprime le brouillon source
- `Bcc` reste stocke a part dans `message_bcc_recipients`
- `Bcc` n'est pas reinjecte dans la recherche, ni dans `participants_normalized`, ni dans `Email/query`
- `Email/get` peut restituer `bcc` uniquement si la propriete `bcc` est explicitement demandee pour le brouillon ou message emetteur du compte authentifie

### Limitations assumees du MVP

- `Email/query` supporte seulement le tri `receivedAt` descendant
- `Email/query` supporte seulement le filtre `inMailbox`
- `Email/get` expose un sous-ensemble utile des proprietes `JMAP Mail`
- un email `LPE` n'est actuellement rattache qu'a une seule mailbox `LPE`, donc `mailboxIds` contient une seule entree
- `EmailSubmission/set` supporte seulement `create`
- `EmailSubmission/set` attend un brouillon existant via `emailId` ou une reference de creation resolue dans la meme requete
- `Mailbox/set` ne permet pas de modifier ni supprimer les mailboxes systeme (`Inbox`, `Sent`, `Drafts`, etc.)
- `Email/copy` supporte seulement la copie dans le meme compte
- `Email/import` consomme un blob `message/rfc822` et applique un parseur `RFC822` minimal pour `From`, `To`, `Cc`, `Subject`, `Message-Id` et le corps texte
- `Blob/upload` stocke actuellement des blobs temporaires en base `PostgreSQL`

### Prochaines methodes a prevoir

- `Blob/copy`
- `Email/queryChanges`
- `Mailbox/queryChanges`
- `Thread/query`
- `VacationResponse/get`
- import MIME plus complet avec support multipart et attachements

## English

### Objective

This document describes the `JMAP Mail` scope currently supported by `LPE` for the MVP.

`crates/lpe-jmap` acts as a `JMAP` adapter on top of the existing canonical `LPE` model implemented in `lpe-storage`. It does not create any parallel `Sent`, `Drafts`, `Outbox`, or transport-side `SMTP` logic.

### Authentication

- the `JMAP` client reuses the existing mailbox-account authentication
- login remains `/api/mail/auth/login`
- the existing account bearer token must then be sent to `/api/jmap/session` and `/api/jmap/api`
- without the Debian reverse proxy, the same routes are exposed directly as `/jmap/session` and `/jmap/api`

### Supported session capabilities

- `urn:ietf:params:jmap:core`
- `urn:ietf:params:jmap:mail`
- `urn:ietf:params:jmap:submission`

The `JMAP` session is real: it is built from the authenticated mailbox account and exposes that current `LPE` account as the active `accountId`.

### Supported methods

- `Mailbox/get`
- `Mailbox/query`
- `Mailbox/changes`
- `Mailbox/set`
- `Email/query`
- `Email/get`
- `Email/changes`
- `Email/set` for draft creation, update, and deletion
- `Email/copy`
- `Email/import`
- `EmailSubmission/get`
- `EmailSubmission/set` for draft submission through the canonical `LPE` submission model
- `Identity/get`
- `Thread/get`
- `Thread/changes`
- `Quota/get`
- `SearchSnippet/get`

Additional supported `JMAP` routes:

- `POST /api/jmap/upload/{accountId}` for temporary `JMAP` blob upload
- `GET /api/jmap/download/{accountId}/{blobId}/{name}` for temporary blob download

### Important MVP rules

- `Email/set` persists only in the `Drafts` mailbox
- `EmailSubmission/set` does not submit raw MIME or direct `SMTP`
- `EmailSubmission/set` takes an existing draft `emailId` and calls the canonical `LPE` submission workflow
- canonical submission creates the authoritative copy in `Sent`, marks the message as `queued`, inserts an `outbound_message_queue` row, then removes the source draft
- `Bcc` remains stored separately in `message_bcc_recipients`
- `Bcc` is not reinjected into search, `participants_normalized`, or `Email/query`
- `Email/get` may return `bcc` only when the `bcc` property is explicitly requested for the authenticated account's own sender-side draft or sent message

### Accepted MVP limitations

- `Email/query` supports only descending `receivedAt` sort
- `Email/query` supports only the `inMailbox` filter
- `Email/get` exposes a practical subset of `JMAP Mail` properties
- one `LPE` email currently belongs to one `LPE` mailbox, so `mailboxIds` contains one entry
- `EmailSubmission/set` currently supports only `create`
- `EmailSubmission/set` expects an existing draft through `emailId` or a resolved creation reference in the same request
- `Mailbox/set` cannot modify or delete system mailboxes (`Inbox`, `Sent`, `Drafts`, etc.)
- `Email/copy` currently supports only same-account copy
- `Email/import` consumes a `message/rfc822` blob and applies a minimal `RFC822` parser for `From`, `To`, `Cc`, `Subject`, `Message-Id`, and plain-text body
- `Blob/upload` currently stores temporary blobs in `PostgreSQL`

### Next methods to add

- `Blob/copy`
- `Email/queryChanges`
- `Mailbox/queryChanges`
- `Thread/query`
- `VacationResponse/get`
- fuller MIME import with multipart and attachment support
