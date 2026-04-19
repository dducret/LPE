# ActiveSync MVP | MVP ActiveSync

## Francais

### Objectif

Ce document decrit le premier adaptateur `ActiveSync` implemente dans `LPE`.

Le crate `crates/lpe-activesync` expose un sous-ensemble pragmatique de `Exchange ActiveSync` pour les usages Outlook/mobile les plus prioritaires, sans introduire de logique parallele de `Sent`, `Outbox` ou de bypass `SMTP`.

### Principes d'architecture

- l'adaptateur `ActiveSync` est separe de `lpe-jmap` et du coeur metier
- l'authentification reutilise le compte mailbox `LPE`
- la synchronisation lit la projection canonique stockee dans `PostgreSQL`
- les brouillons reutilisent `save_draft_message` et `delete_draft_message`
- l'envoi reutilise `submit_message`, qui cree la copie autoritative dans `Sent` puis ajoute l'entree `outbound_message_queue`
- aucun endpoint `ActiveSync` ne parle `SMTP` Internet-facing directement
- `LPE-CT` reste le seul composant responsable du relais `SMTP` sortant
- `EWS` n'est pas implemente

### Endpoints

- `OPTIONS /Microsoft-Server-ActiveSync`
- `POST /Microsoft-Server-ActiveSync`

Sans reverse proxy Debian, ces routes sont exposees directement par le service Rust.

Avec le reverse proxy Debian documente dans le projet, elles sont publiees sur `/Microsoft-Server-ActiveSync`. Le path historique `/api/Microsoft-Server-ActiveSync` reste disponible uniquement si un frontal local continue a tout prefixer sous `/api/`.

### Authentification

- le MVP accepte l'authentification `Basic` sur le compte mailbox
- l'authentification par bearer token de session compte existante reste supportee pour les tests et l'integration interne
- il n'existe pas de compte `ActiveSync` distinct du compte `LPE`

### Protocole et commandes supportees

Le MVP implemente un codec `WBXML` cible sur les code pages necessaires au perimetre courant, puis supporte:

- `Provision`
- `FolderSync`
- `Sync`
- `SendMail`
- `Search`
- `ItemOperations`
- `Ping`
- `SmartReply`
- `SmartForward`

`Sync` tolere aussi plusieurs collections dans une meme requete et accepte sans erreur plusieurs options protocole frequentes dont le MVP n'exploite pas encore toute la semantique:

- `GetChanges`
- `DeletesAsMoves`
- `WindowSize`
- `Options`
- `BodyPreference`
- `Fetch` non mutant cote brouillons

### Perimetre MVP supporte

- authentification compte
- `Provision` minimal avec politique legere et `PolicyKey`
- sync des dossiers de base `Inbox`, `Sent`, `Drafts`
- exposition des collections `Contacts` et `Calendar`
- sync des messages `Inbox`, `Sent`, `Drafts`
- creation, modification et suppression de brouillons via `Sync` sur `Drafts`
- mutations `Contacts` via `Sync` sur la collection `Contacts`
- mutations `Calendar` via `Sync` sur la collection `Calendar`
- envoi via `SendMail`, branche sur la soumission canonique `LPE`
- pieces jointes `SendMail` validees par `Magika`, persistees dans le modele canonique et exposees via `Sync` + `ItemOperations`
- recherche mailbox `Search` branchee sur la projection canonique `PostgreSQL`, y compris l'indexation texte des pieces jointes v1 deja supportees
- recuperation `ItemOperations` des messages et pieces jointes par `FileReference`
- `Ping` sur dossiers synchronises en comparant l'etat courant aux derniers `SyncKey` persistants du device
- `SmartReply` et `SmartForward` branches sur la soumission canonique, avec reutilisation du message source `LPE` et reprise des pieces jointes source pour le forward
- garantie qu'un message envoye depuis le client natif est visible dans `Sent`
- persistance des `SyncKey` par compte, appareil et collection en base `PostgreSQL`
- pagination `Sync` complete avec `WindowSize` et `MoreAvailable`, y compris la continuation d'un lot sur plusieurs `SyncKey`
- parsing `SendMail` durci pour les clients natifs: en-tetes replies, sujets et noms affiches RFC 2047, corps texte `quoted-printable`, `base64` et `multipart/alternative`

### Contacts et calendrier

Le MVP expose `Contacts` et `Calendar` pour la synchronisation descendante et supporte maintenant les mutations de base cote client via `Sync`:

- `Add`
- `Change`
- `Delete`

Ces mutations restent branchees directement sur les modeles canoniques `contacts` et `calendar_events`.

### Regles importantes

- `Sent` reste la vue autoritative; `ActiveSync` n'ecrit pas sa propre copie
- `SendMail` finit toujours dans la soumission canonique `LPE`
- la soumission canonique reste transactionnelle: message stocke, copie `Sent`, queue sortante, puis relais via `LPE-CT`
- les metadonnees `Bcc` ne sont pas reinjectees dans la recherche standard
- l'adaptateur ne reutilise aucun code `Stalwart`

### Limitations connues

- le MVP ne couvre pas `EWS`
- le parseur `WBXML` est volontairement limite aux tags utilises par ce MVP
- la recherche `Search` reste volontairement limitee au store mailbox canonique `LPE`; elle ne couvre pas `GAL`, `DocumentLibrary` ni les operateurs de requete riches
- `ItemOperations` est actuellement limite a `Fetch` pour les messages et les pieces jointes; les autres operations du namespace ne sont pas exposees
- `Ping` est implemente comme un detecteur d'ecart entre l'etat courant et le dernier etat `Sync` persiste du device; le MVP ne maintient pas encore une boucle longue duree ni d'optimisation serveur sophistiquee
- `SmartReply` et `SmartForward` restent cibles pour les usages Outlook/mobile prioritaires: ils reutilisent la soumission canonique et le message source, mais pas encore toute la richesse des variantes ComposeMail
- le parseur `MIME` de `SendMail` reste volontairement limite au MVP: il gere maintenant les pieces jointes MIME usuelles, mais pas toute la richesse MIME
- la gestion fine des mises a jour partielles cote client est actuellement concentree sur `Drafts`
- la sync `Drafts` est ciblee pour un usage `ActiveSync 16.1`; les clients limites aux anciennes versions ne doivent pas etre consideres comme pleinement supportes pour ce point
- le premier `Sync` avec `SyncKey = 0` suit un amorcage prudent: il etablit un nouvel etat serveur avant d'emettre les pages suivantes, ce qui est cible pour Outlook/mobile mais n'a pas encore ete eprouve contre toute la diversite des clients `ActiveSync`

## English

### Objective

This document describes the first `ActiveSync` adapter implemented in `LPE`.

The `crates/lpe-activesync` crate exposes a pragmatic subset of `Exchange ActiveSync` for the most important Outlook and mobile scenarios, without introducing any parallel `Sent`, `Outbox`, or direct `SMTP` bypass logic.

### Architectural principles

- the `ActiveSync` adapter is separated from `lpe-jmap` and from the business core
- authentication reuses the mailbox account already defined in `LPE`
- synchronization reads the canonical mailbox projection stored in `PostgreSQL`
- future task synchronization must reuse the canonical `tasks` model instead of introducing an `ActiveSync`-specific task store
- drafts reuse `save_draft_message` and `delete_draft_message`
- message submission reuses `submit_message`, which writes the authoritative `Sent` copy and then appends the `outbound_message_queue` row
- no `ActiveSync` endpoint performs direct Internet-facing `SMTP`
- `LPE-CT` remains the only component responsible for outbound `SMTP` relay
- `EWS` is not implemented

### Endpoints

- `OPTIONS /Microsoft-Server-ActiveSync`
- `POST /Microsoft-Server-ActiveSync`

Without the Debian reverse proxy, these routes are exposed directly by the Rust service.

With the documented Debian reverse proxy, they are published on `/Microsoft-Server-ActiveSync`. The historical `/api/Microsoft-Server-ActiveSync` path remains available only when a local front end still prefixes every upstream route under `/api/`.

### Authentication

- the MVP accepts mailbox-account `Basic` authentication
- existing mailbox bearer-session authentication is still supported for tests and internal integration
- there is no separate `ActiveSync` account model outside the normal `LPE` mailbox account

### Supported protocol commands

The MVP implements a focused `WBXML` codec for the code pages needed by the current scope, then supports:

- `Provision`
- `FolderSync`
- `Sync`
- `SendMail`
- `Search`
- `ItemOperations`
- `Ping`
- `SmartReply`
- `SmartForward`

`Sync` also tolerates multiple collections in the same request and accepts several common protocol options without failing even when the MVP does not yet implement their full semantics:

- `GetChanges`
- `DeletesAsMoves`
- `WindowSize`
- `Options`
- `BodyPreference`
- non-mutating draft-side `Fetch`

### Supported MVP scope

- account authentication
- minimal `Provision` flow with a lightweight device policy and `PolicyKey`
- base-folder synchronization for `Inbox`, `Sent`, and `Drafts`
- exposure of `Contacts` and `Calendar` collections
- message synchronization for `Inbox`, `Sent`, and `Drafts`
- draft creation, update, and deletion through `Sync` on `Drafts`
- `Contacts` mutations through `Sync` on the `Contacts` collection
- `Calendar` mutations through `Sync` on the `Calendar` collection
- message submission through `SendMail`, wired to the canonical `LPE` submission workflow
- `SendMail` attachments validated through `Magika`, persisted through the canonical model, and exposed back through `Sync` + `ItemOperations`
- mailbox `Search` wired to the canonical `PostgreSQL` projection, including the v1 attachment text index already supported by `LPE`
- `ItemOperations` retrieval for messages and attachment payloads through canonical `FileReference` values
- `Ping` over synchronized folders by comparing current collection state against the device's latest persisted `SyncKey`
- `SmartReply` and `SmartForward` wired to canonical submission, reusing the canonical source message and forwarding source attachments when needed
- guarantee that a message sent from a native client becomes visible in the authoritative `Sent` view
- persistent `SyncKey` storage in `PostgreSQL` per account, device, and collection
- complete `Sync` pagination with `WindowSize` and `MoreAvailable`, including continuation of a server batch across multiple `SyncKey` values
- incremental `Sync` state tracking with compact per-item fingerprints instead of full serialized `ApplicationData` snapshots for large mailbox collections
- hardened `SendMail` parsing for native clients: folded headers, RFC 2047 encoded subjects and display names, `quoted-printable`, `base64`, and `multipart/alternative` text bodies

### Contacts and calendar

The MVP exposes `Contacts` and `Calendar` for downstream synchronization and now supports basic client-originated mutations through `Sync`:

- `Add`
- `Change`
- `Delete`

Those mutations still write directly into the canonical `contacts` and `calendar_events` models.

### Important rules

- `Sent` remains authoritative; `ActiveSync` does not write a parallel sent copy
- `SendMail` always finishes in canonical `LPE` submission
- canonical submission remains transactional: message stored, `Sent` copy written, outbound queue persisted, then relay delegated to `LPE-CT`
- `Bcc` metadata is not reinjected into standard mailbox search
- the adapter does not reuse any `Stalwart` code

### Known limitations

- the MVP does not implement `EWS`
- the MVP does not expose the `Tasks` class yet; future task support must build on `docs/architecture/tasks-mvp.md`
- the `WBXML` parser is intentionally limited to the tags used by this MVP
- `Search` is intentionally limited to the canonical mailbox store; it does not cover `GAL`, `DocumentLibrary`, or richer search operators yet
- `ItemOperations` is currently limited to `Fetch` for messages and attachments; the other namespace operations are not exposed
- `Ping` is implemented as a lightweight delta detector against the device's latest persisted sync state; the MVP does not yet implement a sophisticated long-lived push loop
- `SmartReply` and `SmartForward` are targeted at the highest-priority Outlook/mobile flows; they reuse canonical submission and source-message data, but they do not yet cover every ComposeMail variant
- the `SendMail` `MIME` parser is still intentionally limited to MVP needs: it now covers common MIME attachments, but not the full MIME surface
- fine-grained client-originated mutation handling is currently focused on `Drafts`
- `Drafts` synchronization is targeted for `ActiveSync 16.1`; clients limited to older protocol versions should not be treated as fully supported for that capability
- the first `Sync` with `SyncKey = 0` uses a conservative priming round-trip before emitting the paged server changes; this is targeted for Outlook/mobile but has not yet been validated against the full diversity of `ActiveSync` clients
- `Sync` continuation is stabilized around a compact collection fingerprint set rather than persisted full payload snapshots; if an item targeted by an unfinished paged batch mutates before that page is emitted, the server may invalidate that continuation `SyncKey` and require a fresh sync instead of replaying a stale payload
