# La Poste Electronique

## Francais

`LPE` est une plateforme de messagerie et de collaboration moderne, multiplateforme, ecrite majoritairement en Rust.

### Principes initiaux

- code projet sous licence `Apache-2.0`
- dependances `MIT` autorisees uniquement si aucune alternative `Apache-2.0` raisonnable n'existe
- `PostgreSQL` comme stockage primaire de metadonnees
- `JMAP` comme axe principal du produit moderne
- `IMAP` comme couche de compatibilite mailbox
- transport `SMTP` entrant et sortant porte par le centre de tri `LPE-CT`
- compatibilite client native visee via `IMAP`
- `ActiveSync` cible comme premiere couche de compatibilite native Outlook/mobile
- `EWS` reserve comme extension future apres stabilisation du modele canonique de soumission et de synchronisation
- architecture preparee pour une IA locale future sans sortie de donnees hors serveur

### Structure

- `crates/` services et bibliotheques Rust, avec `lpe-storage` qui centralise aussi le parsing mail partage utilise par les imports et les adaptateurs protocolaires
- `web/admin` back office React/TypeScript
- `web/client` client web type Outlook Web avec shell mail, liste de messages et panneau de lecture
- `LPE-CT/` centre de tri DMZ separe avec sa propre documentation et ses scripts Debian
- `docs/architecture` decisions techniques initiales
- `docs/licensing` politique de licences et garde-fous CI
- `installation` scripts et documentation de deploiement

### Demarrage

Le squelette actuel permet de compiler les crates Rust du workspace.

```powershell
cargo check
```

Sur `Debian Trixie`, les scripts d'installation deploient aussi la console d'administration statique via `nginx` sur `/`, le client web sur `/mail/`, avec reverse proxy `/api/` vers le service Rust local.

Pour un serveur de tri distinct en `DMZ`, le sous-repertoire `LPE-CT/` fournit un composant autonome avec:

- une API Rust de management
- une interface statique dediee
- des scripts `Debian Trixie` d'installation et de mise a jour dans `LPE-CT/installation/debian-trixie`

La console d'administration actuelle couvre deja une V1 de pilotage du plan de controle:

- authentification administrateur avec sessions persistantes, audit de connexion et controle des droits cote API
- login admin local par mot de passe avec premier `TOTP` exploitable et premier login federé `OIDC` pour le back office
- page serveur avec etat, politiques, domaines et administrateurs
- page domaine orientee gestion des comptes avec mot de passe, alias et delegations
- demandes et execution testable d'import/export `PST` au niveau des boites aux lettres depuis la page domaine, avec upload navigateur pour les imports
- page antispam avec moteur, regles et quarantaine
- page audit et compliance avec journal et recherche de trace email
- page operations pour protocoles et stockage

Cette console est maintenant persistante dans `PostgreSQL` via `lpe-storage` et les migrations SQL du projet.

Le protocole moderne principal reste `JMAP`, mais `LPE` doit aussi rester compatible avec des clients natifs. Cela vise en particulier les usages de type application Mail sur iPhone et les clients Outlook. Le support Outlook natif est critique pour l'adoption et ne doit pas etre reduit a `IMAP` + `SMTP` + autodiscover. `ActiveSync` est la premiere couche de compatibilite native Outlook/mobile visee. `EWS` reste une extension future, a evaluer apres stabilisation du modele canonique de soumission et de synchronisation. Dans tous les cas, un message envoye depuis un client externe doit etre enregistre dans `LPE` et rester visible dans la vue `Sent` de maniere coherente sur tous les acces.

Le backend expose un premier modele de soumission canonique via `/api/mail/messages/submit`: un message soumis est stocke dans `messages`, ses destinataires visibles (`To`, `Cc`) sont stockes dans `message_recipients`, les destinataires `Bcc` sont conserves comme metadonnees protegees distinctes pour audit/compliance, sa copie autoritative est placee dans la mailbox `Sent`, puis une entree `outbound_message_queue` prepare la remise sortante via le centre de tri `LPE-CT`.

L'integration fonctionnelle entre `LPE` et `LPE-CT` repose maintenant sur un contrat HTTP interne explicite: un worker `LPE` consomme `outbound_message_queue` et appelle `LPE-CT` pour le handoff sortant, tandis que `LPE-CT` appelle `LPE` pour la remise finale entrante vers les mailbox du `LAN`. Ce contrat couvre maintenant aussi les retries enrichis, les retours `DSN`/bounce, les regles de routage, le throttling sortant et un statut technique structure persiste cote `LPE`. Le contrat et les variables d'environnement associees sont documentes dans `docs/architecture/lpe-ct-integration.md`.

Toutes les couches clientes doivent utiliser le modele canonique `LPE` de soumission et de synchronisation. Aucune couche cliente ne doit ecrire une logique `Sent` ou `Outbox` parallele.

Tout fichier entrant via connexion externe ou via un client doit etre valide par Google `Magika` avant traitement normal. Cette validation s'applique notamment aux pieces jointes mail, uploads navigateur, blobs `JMAP`, imports `PST` et futurs flux de fichiers; elle sert a comparer le type detecte avec le MIME/extension declares avant acceptation, restriction, quarantaine ou rejet.

Le crate `lpe-jmap` expose maintenant un MVP `JMAP Mail` branche sur ce modele canonique. La session `JMAP` reutilise l'authentification compte existante, puis supporte `Mailbox/get`, `Mailbox/query`, `Mailbox/changes`, `Mailbox/set`, `Email/query`, `Email/get`, `Email/changes`, `Email/set`, `Email/copy`, `Email/import`, `EmailSubmission/get`, `EmailSubmission/set`, `Identity/get`, `Thread/get`, `Thread/changes`, `Quota/get` et `SearchSnippet/get`, ainsi qu'un upload/download de blob `JMAP` temporaire. Le scope detaille et les limites du MVP sont documentes dans `docs/architecture/jmap-mail-mvp.md`.

Le meme crate expose maintenant un MVP `JMAP Contacts` et `JMAP Calendars` branche sur les modeles canoniques `contacts` et `calendar_events` deja reutilises par `DAV` et `ActiveSync`. Il supporte `AddressBook/get|query|changes`, `ContactCard/get|query|changes|set`, `Calendar/get|query|changes` et `CalendarEvent/get|query|changes|set`, avec droits restreints au compte authentifie, sans stockage ni logique paralleles. Le scope detaille et les limites du MVP sont documentes dans `docs/architecture/jmap-contacts-calendars-mvp.md`.

Le crate `lpe-imap` expose maintenant un premier serveur `IMAP` MVP branche sur le meme stockage canonique. Il supporte l'authentification compte, `LIST`, `SELECT`, `FETCH`, `STORE`, `SEARCH`, `UID` et `APPEND` vers `Drafts`, sans logique parallele de `Sent`, `Drafts` ou `Outbox`. Le scope detaille et les limites du MVP sont documentes dans `docs/architecture/imap-mvp.md`.

`LPE` expose maintenant un premier MVP `Sieve` et `ManageSieve` aligne sur la livraison canonique entrante. Les scripts sont stockes par compte dans `PostgreSQL`, un seul script actif est autorise par compte, `ManageSieve` fournit un serveur minimal d'administration des scripts, et l'execution cote `LPE` supporte `fileinto`, `discard`, `redirect` et `vacation` sans deplacer la logique de bord vers `LPE-CT`. Le scope detaille et les limites du MVP sont documentes dans `docs/architecture/sieve-managesieve-mvp.md`.

Le crate `lpe-activesync` expose maintenant un premier adaptateur `ActiveSync` MVP. Il supporte l'authentification compte, `Provision`, `FolderSync`, `Sync`, `SendMail`, `Search`, `ItemOperations`, `Ping`, `SmartReply` et `SmartForward`, reutilise les workflows canoniques `LPE` pour les brouillons et la soumission, expose `Inbox`, `Sent`, `Drafts`, `Contacts` et `Calendar`, persiste les pieces jointes dans le modele canonique, et garantit la coherence de `Sent` en ecrivant d'abord la copie autoritative via `submit_message` avant la remise sortante par `LPE-CT`. Le scope detaille et les limites du MVP sont documentes dans `docs/architecture/activesync-mvp.md`.

L'autoconfiguration client publie maintenant des endpoints MVP reels pour `Thunderbird` et `Outlook`: `Thunderbird` recupere l'acces `IMAP` via XML autoconfig, tandis que l'autodiscover `Outlook` minimal publie l'endpoint `ActiveSync`. Le relais `SMTP` interne `LPE -> LPE-CT` reste distinct et n'est pas annonce comme soumission client tant qu'un endpoint authentifie de publication n'est pas explicitement configure.

Le crate `lpe-dav` expose maintenant un adaptateur d'interoperabilite `CardDAV` et `CalDAV` MVP. Il reutilise l'authentification compte deja utilisee par le webmail et `ActiveSync`, expose les contacts et evenements canoniques stockes dans `contacts` et `calendar_events`, supporte `PROPFIND`, `REPORT`, `GET`, `PUT`, `DELETE` et `OPTIONS` avec `ETag` conditionnels, filtres `REPORT` plus realistes et interop calendrier minimale (`TZID`, `DURATION`, `RRULE`, attendees structures), et reste une couche de compatibilite sans logique metier parallele. Le scope detaille et les limites du MVP sont documentes dans `docs/architecture/dav-mvp.md`.

`LPE` expose maintenant un premier modele canonique des taches personnelles via la table `tasks`, des endpoints comptes `/api/mail/tasks`, et une inclusion dans `/api/mail/workspace`, sans logique parallele par protocole. Le modele MVP et ses limites sont documentes dans `docs/architecture/tasks-mvp.md`.

Le client web `/mail/` exige une authentification utilisateur. Les identifiants sont ceux du compte cree dans l'administration; le mot de passe est hache cote serveur avec `argon2`, stocke dans `account_credentials`, puis valide via `/api/mail/auth/login` avant d'ouvrir l'interface. Apres connexion, le client charge les messages, contacts, evenements et taches depuis `/api/mail/workspace`; la composition, l'enregistrement, la modification, la suppression et l'expedition de brouillons, les contacts, les entrees calendrier et les taches utilisent des endpoints persistants et n'affichent plus de donnees de maquette.

### Axe IA locale

`LPE` prepare des projections documentaires canoniques, des chunks et des annotations qui serviront plus tard a une integration LLM locale type Gemma, sans rendre le moteur IA dependance du coeur metier.

### Formats documentaires v1

Les pieces jointes suivantes sont prevues pour l'indexation texte en v1:

- `PDF` via `pdf_oxide`
- `DOCX` via `docx-lite`
- `ODT` via un extracteur ODF minimal focalise texte

### Interfaces multilingues

Les interfaces web supportent en v1:

- anglais par defaut
- francais
- allemand
- italien
- espagnol

## English

`LPE` is a modern cross-platform mail and collaboration platform written primarily in Rust.

### Initial principles

- project code is licensed under `Apache-2.0`
- `MIT` dependencies are allowed only when no reasonable `Apache-2.0` alternative exists
- `PostgreSQL` is the primary metadata store
- `JMAP` is the main protocol axis for the modern product
- `IMAP` is a mailbox compatibility layer
- inbound and outbound `SMTP` transport is handled by the `LPE-CT` sorting center
- native client compatibility is a target through `IMAP`
- `ActiveSync` is the first targeted native Outlook and mobile compatibility layer
- `EWS` is reserved as a future extension after the canonical submission and synchronization model is stabilized
- the architecture is prepared for future local AI without data leaving the server

### Structure

- `crates/` Rust services and libraries, with `lpe-storage` also centralizing shared mail parsing used by imports and protocol adapters
- `web/admin` React/TypeScript back office
- `web/client` Outlook Web style client with a mail shell, message list, and reading pane
- `LPE-CT/` separate DMZ sorting center with its own documentation and Debian scripts
- `docs/architecture` initial technical decisions
- `docs/licensing` license policy and CI guardrails
- `installation` deployment scripts and documentation

### Getting started

The current skeleton compiles the Rust workspace crates.

```powershell
cargo check
```

On `Debian Trixie`, the installation scripts also deploy the static administration console through `nginx` on `/`, the web client on `/mail/`, with `/api/` reverse-proxied to the local Rust service.

For a separate sorting server placed in a `DMZ`, the `LPE-CT/` subdirectory provides an autonomous component with:

- a Rust management API
- a dedicated static interface
- `Debian Trixie` install and update scripts in `LPE-CT/installation/debian-trixie`

The current administration console already exposes a first control-plane V1:

- administrator authentication with persistent sessions, sign-in audit, and API-side rights checks
- local password admin login with a first usable `TOTP` flow and a first federated `OIDC` login for the back office
- server page with status, policies, domains, and administrators
- domain-oriented page for accounts with passwords, aliases, and delegated administration
- mailbox-level `PST` import/export requests and testable execution from the domain page, with browser upload for imports
- anti-spam page with engine, rules, and quarantine
- audit and compliance page with journal and email trace search
- operations page for protocols and storage

This console is now persisted in `PostgreSQL` through `lpe-storage` and the project's SQL migrations.

The main modern protocol remains `JMAP`, but `LPE` must also stay compatible with native clients. This especially targets use cases such as the iPhone Mail application and Outlook clients. Native Outlook support is critical for adoption and must not be reduced to `IMAP` + `SMTP` + autodiscover. `ActiveSync` is the first targeted native Outlook and mobile compatibility layer. `EWS` remains a future extension to evaluate after the canonical submission and synchronization model is stabilized. In every case, a message sent from an external client must be recorded in `LPE` and remain visible in the authoritative `Sent` view across access paths.

The backend now exposes an initial canonical submission model through `/api/mail/messages/submit`: a submitted message is stored in `messages`, visible recipients (`To`, `Cc`) are stored in `message_recipients`, `Bcc` recipients are retained as separate protected metadata for audit and compliance, the authoritative copy is placed in the `Sent` mailbox, and an `outbound_message_queue` entry prepares outbound handoff through the `LPE-CT` sorting center.

The working integration between `LPE` and `LPE-CT` now relies on an explicit internal HTTP contract: an `LPE` worker consumes `outbound_message_queue` and calls `LPE-CT` for outbound handoff, while `LPE-CT` calls back into `LPE` for final inbound delivery into LAN-hosted mailboxes. That contract now also covers richer retries, `DSN`/bounce feedback, outbound routing rules, outbound throttling, and a structured technical status persisted on the `LPE` side. The contract and its environment variables are documented in `docs/architecture/lpe-ct-integration.md`.

`LPE` and `LPE-CT` now also expose a first modern observability layer: Prometheus-compatible `/metrics` endpoints, structured tracing with optional JSON logs, and cross-service correlation through `trace_id` and `message_id`. The operational model is documented in `docs/architecture/observability.md`.

All client layers must use the canonical `LPE` submission and synchronization model. No client layer may write its own parallel `Sent` or `Outbox` logic.

Every file entering through an external connection or through a client must be validated with Google `Magika` before normal processing. This validation applies in particular to mail attachments, browser uploads, `JMAP` blobs, `PST` imports, and future file-ingress workflows; it is used to compare the detected content type with the declared MIME type and extension before accept, restrict, quarantine, or reject decisions.

The `lpe-jmap` crate now exposes a first `JMAP Mail` MVP wired to that canonical model. The `JMAP` session reuses the existing mailbox-account authentication, then supports `Mailbox/get`, `Mailbox/query`, `Mailbox/changes`, `Mailbox/set`, `Email/query`, `Email/get`, `Email/changes`, `Email/set`, `Email/copy`, `Email/import`, `EmailSubmission/get`, `EmailSubmission/set`, `Identity/get`, `Thread/get`, `Thread/changes`, `Quota/get`, and `SearchSnippet/get`, together with temporary `JMAP` blob upload/download. The supported scope and limitations are documented in `docs/architecture/jmap-mail-mvp.md`.

The same crate now also exposes an MVP `JMAP Contacts` and `JMAP Calendars` layer wired to the canonical `contacts` and `calendar_events` models already reused by `DAV` and `ActiveSync`. It supports `AddressBook/get|query|changes`, `ContactCard/get|query|changes|set`, `Calendar/get|query|changes`, and `CalendarEvent/get|query|changes|set`, with rights bounded by the authenticated account and without introducing parallel storage or business logic. The supported scope and limitations are documented in `docs/architecture/jmap-contacts-calendars-mvp.md`.

The `lpe-imap` crate now exposes a first `IMAP` MVP server wired to the same canonical store. It supports account authentication, `LIST`, `SELECT`, `FETCH`, `STORE`, `SEARCH`, `UID`, and `APPEND` to `Drafts`, without introducing parallel `Sent`, `Drafts`, or `Outbox` logic. The supported scope and limitations are documented in `docs/architecture/imap-mvp.md`.

`LPE` now also exposes a first `Sieve` and `ManageSieve` MVP aligned with canonical inbound delivery. Scripts are stored per account in `PostgreSQL`, only one active script is allowed per account, a minimal `ManageSieve` server handles script administration, and `LPE` executes `fileinto`, `discard`, `redirect`, and `vacation` during final inbound delivery without moving edge policy logic back into `LPE-CT`. The supported scope and limitations are documented in `docs/architecture/sieve-managesieve-mvp.md`.

The `lpe-activesync` crate now exposes a first `ActiveSync` MVP adapter. It supports account authentication, `Provision`, `FolderSync`, `Sync`, `SendMail`, `Search`, `ItemOperations`, `Ping`, `SmartReply`, and `SmartForward`, reuses the canonical `LPE` draft and submission workflows, exposes `Inbox`, `Sent`, `Drafts`, `Contacts`, and `Calendar`, persists attachments through the canonical model, and keeps sent-message consistency by writing the authoritative `Sent` copy through `submit_message` before outbound relay is handed to `LPE-CT`. The supported scope and limitations are documented in `docs/architecture/activesync-mvp.md`.

Client auto-configuration now publishes real MVP endpoints for `Thunderbird` and `Outlook`: `Thunderbird` receives `IMAP` settings through XML autoconfig, while minimal `Outlook` autodiscovery publishes the implemented `ActiveSync` endpoint. The internal `LPE -> LPE-CT` `SMTP` relay remains separate and is not advertised as a client-submission endpoint unless an authenticated public submission service is explicitly configured.

The `lpe-dav` crate now exposes a `CardDAV` and `CalDAV` interoperability MVP adapter. It reuses the mailbox-account authentication already used by the web client and `ActiveSync`, exposes the canonical collaboration data stored in `contacts` and `calendar_events`, supports `PROPFIND`, `REPORT`, `GET`, `PUT`, `DELETE`, and `OPTIONS` with conditional `ETag` handling, more realistic `REPORT` filtering, and minimal calendar interoperability (`TZID`, `DURATION`, `RRULE`, structured attendees), and remains a compatibility layer rather than a parallel business model. The supported scope and limitations are documented in `docs/architecture/dav-mvp.md`.

`LPE` now exposes a first canonical personal-task model through the `tasks` table, account-scoped `/api/mail/tasks` endpoints, and inclusion in `/api/mail/workspace`, without introducing protocol-specific parallel logic. The MVP model and its limits are documented in `docs/architecture/tasks-mvp.md`.

The `/mail/` web client requires user authentication. Credentials are the mailbox account credentials created in the administration console; the password is hashed server-side with `argon2`, stored in `account_credentials`, and validated through `/api/mail/auth/login` before the interface opens. After sign-in, the client loads messages, contacts, events, and tasks from `/api/mail/workspace`; composition, draft saving, draft editing, draft deletion, draft sending, contacts, calendar entries, and tasks use persistent endpoints and no longer display mock seed data.

### Local AI direction

`LPE` prepares canonical document projections, chunks, and annotations so a future local LLM such as Gemma can be integrated without making the AI engine a core product dependency.

### v1 document formats

The following attachment formats are planned for text indexing in v1:

- `PDF` via `pdf_oxide`
- `DOCX` via `docx-lite`
- `ODT` via a minimal text-focused ODF extractor

### Multilingual interfaces

The web interfaces support the following languages in v1:

- English by default
- French
- German
- Italian
- Spanish
