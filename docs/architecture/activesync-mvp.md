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

Avec le reverse proxy `/api/` documente dans le projet, elles sont atteignables via `/api/Microsoft-Server-ActiveSync`.

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

### Perimetre MVP supporte

- authentification compte
- `Provision` minimal avec politique legere et `PolicyKey`
- sync des dossiers de base `Inbox`, `Sent`, `Drafts`
- exposition des collections `Contacts` et `Calendar`
- sync des messages `Inbox`, `Sent`, `Drafts`
- creation, modification et suppression de brouillons via `Sync` sur `Drafts`
- envoi via `SendMail`, branche sur la soumission canonique `LPE`
- garantie qu'un message envoye depuis le client natif est visible dans `Sent`
- persistance des `SyncKey` par compte, appareil et collection en base `PostgreSQL`

### Contacts et calendrier

Le MVP expose `Contacts` et `Calendar` en lecture pour la synchronisation descendante.

La creation, modification et suppression cote client pour ces deux classes ne sont pas encore implementees afin de limiter le scope du premier adaptateur.

### Regles importantes

- `Sent` reste la vue autoritative; `ActiveSync` n'ecrit pas sa propre copie
- `SendMail` finit toujours dans la soumission canonique `LPE`
- la soumission canonique reste transactionnelle: message stocke, copie `Sent`, queue sortante, puis relais via `LPE-CT`
- les metadonnees `Bcc` ne sont pas reinjectees dans la recherche standard
- l'adaptateur ne reutilise aucun code `Stalwart`

### Limitations connues

- le MVP ne couvre pas `EWS`
- le MVP ne couvre pas `SmartReply`, `SmartForward`, `ItemOperations`, `Ping`, `Search` ni les pieces jointes ActiveSync
- le parseur `WBXML` est volontairement limite aux tags utilises par ce MVP
- le parseur `MIME` de `SendMail` est minimal: il traite `To`, `Cc`, `Bcc`, `Subject`, `Message-Id` et le corps texte
- la sync `Contacts` et `Calendar` est descendante uniquement
- la gestion fine des mises a jour partielles cote client est actuellement concentree sur `Drafts`
- la sync `Drafts` est ciblee pour un usage `ActiveSync 16.1`; les clients limites aux anciennes versions ne doivent pas etre consideres comme pleinement supportes pour ce point

## English

### Objective

This document describes the first `ActiveSync` adapter implemented in `LPE`.

The `crates/lpe-activesync` crate exposes a pragmatic subset of `Exchange ActiveSync` for the most important Outlook and mobile scenarios, without introducing any parallel `Sent`, `Outbox`, or direct `SMTP` bypass logic.

### Architectural principles

- the `ActiveSync` adapter is separated from `lpe-jmap` and from the business core
- authentication reuses the mailbox account already defined in `LPE`
- synchronization reads the canonical mailbox projection stored in `PostgreSQL`
- drafts reuse `save_draft_message` and `delete_draft_message`
- message submission reuses `submit_message`, which writes the authoritative `Sent` copy and then appends the `outbound_message_queue` row
- no `ActiveSync` endpoint performs direct Internet-facing `SMTP`
- `LPE-CT` remains the only component responsible for outbound `SMTP` relay
- `EWS` is not implemented

### Endpoints

- `OPTIONS /Microsoft-Server-ActiveSync`
- `POST /Microsoft-Server-ActiveSync`

Without the Debian reverse proxy, these routes are exposed directly by the Rust service.

With the documented `/api/` reverse proxy, they are reachable through `/api/Microsoft-Server-ActiveSync`.

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

### Supported MVP scope

- account authentication
- minimal `Provision` flow with a lightweight device policy and `PolicyKey`
- base-folder synchronization for `Inbox`, `Sent`, and `Drafts`
- exposure of `Contacts` and `Calendar` collections
- message synchronization for `Inbox`, `Sent`, and `Drafts`
- draft creation, update, and deletion through `Sync` on `Drafts`
- message submission through `SendMail`, wired to the canonical `LPE` submission workflow
- guarantee that a message sent from a native client becomes visible in the authoritative `Sent` view
- persistent `SyncKey` storage in `PostgreSQL` per account, device, and collection

### Contacts and calendar

The MVP exposes `Contacts` and `Calendar` for read-only downstream synchronization.

Client-side create, update, and delete operations for those two classes are intentionally left out of the first adapter to keep the initial scope controlled.

### Important rules

- `Sent` remains authoritative; `ActiveSync` does not write a parallel sent copy
- `SendMail` always finishes in canonical `LPE` submission
- canonical submission remains transactional: message stored, `Sent` copy written, outbound queue persisted, then relay delegated to `LPE-CT`
- `Bcc` metadata is not reinjected into standard mailbox search
- the adapter does not reuse any `Stalwart` code

### Known limitations

- the MVP does not implement `EWS`
- the MVP does not implement `SmartReply`, `SmartForward`, `ItemOperations`, `Ping`, `Search`, or ActiveSync attachment retrieval
- the `WBXML` parser is intentionally limited to the tags used by this MVP
- the `SendMail` `MIME` parser is minimal and currently handles `To`, `Cc`, `Bcc`, `Subject`, `Message-Id`, and plain-text body
- `Contacts` and `Calendar` synchronization is read-only in this first step
- fine-grained client-originated mutation handling is currently focused on `Drafts`
- `Drafts` synchronization is targeted for `ActiveSync 16.1`; clients limited to older protocol versions should not be treated as fully supported for that capability
