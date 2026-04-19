# Architecture initiale | Initial architecture

## Francais

### Decisions de depart

- stockage primaire : `PostgreSQL`
- axe protocolaire moderne : `JMAP`
- compatibilite initiale : `IMAP`; transport `SMTP` expose par le centre de tri `LPE-CT`
- code LPE : `Apache-2.0`
- dependances : `Apache-2.0` d'abord, `MIT` uniquement par exception justifiee
- architecture de donnees preparee pour une IA locale future
- centre de tri `LPE-CT` distinct pour le transport `SMTP` entrant et sortant expose, deployable en `DMZ` sans deplacer le coeur metier

### Vision

`LPE` est un serveur de messagerie et de collaboration moderne. Le coeur metier ne depend pas de `IMAP` ni de `SMTP`. Les protocoles externes sont des adaptateurs autour d'un modele interne stable.

Les clients natifs restent un objectif important. Un utilisateur doit pouvoir connecter sa boite `LPE` depuis un client mobile ou desktop compatible, par exemple l'application Mail de l'iPhone, sans perdre la coherence de sa boite entre protocoles.

Cela implique que tout envoi realise via un protocole client supporte, notamment `JMAP`, `IMAP` ou `ActiveSync`, alimente la meme representation canonique du message dans `LPE`, y compris la copie du message envoye dans la vue `Sent`. L'execution du transport `SMTP` entrant et sortant reste une responsabilite du centre de tri.

`ActiveSync` est la premiere couche de compatibilite native Outlook/mobile visee. `EWS` reste une extension future, a evaluer apres stabilisation du modele canonique de soumission et de synchronisation.

`CalDAV` et `CardDAV` sont des couches de compatibilite standards pour les donnees de collaboration. Ils doivent rester branches sur les modeles canoniques `LPE` pour les contacts et le calendrier, sans introduire de stockage, de droits ou de logique metier paralleles.

Le modele de soumission initial est transactionnel cote coeur `LPE` et expose par l'API `/api/mail/messages/submit`:

1. verifier le compte emetteur
2. garantir l'existence de la mailbox `Sent`
3. creer le message canonique dans `messages`
4. enregistrer les destinataires visibles dans `message_recipients` et conserver `Bcc` dans un stockage protege distinct
5. indexer le corps dans `message_bodies` sans inclure `Bcc` dans `participants_normalized`
6. ajouter une entree `outbound_message_queue` pour remise via `LPE-CT`
7. journaliser l'action dans `audit_events`

Cette sequence donne a `Sent` le statut de source autoritative avant meme la remise SMTP effective par le centre de tri.

L'integration fonctionnelle v1 entre le coeur et le centre de tri est maintenant explicite:

- un worker `LPE` lit `outbound_message_queue` et appelle `LPE-CT`
- `LPE-CT` retourne un statut de transport parmi `queued`, `relayed`, `deferred`, `quarantined`, `failed`
- `LPE-CT` remet les messages entrants acceptes vers `LPE` via une API interne de livraison finale

Le detail du contrat est documente dans `docs/architecture/lpe-ct-integration.md`.

Toutes les couches clientes doivent utiliser ce modele canonique de soumission et de synchronisation. Aucune couche cliente ne doit ecrire une logique `Sent` ou `Outbox` parallele.

Tout fichier entrant via connexion externe ou via un client doit etre valide par Google `Magika` avant traitement normal. Cela s'applique a `LPE-CT` pour les flux externes et a `LPE` pour les uploads et imports clients.

Le MVP `JMAP Mail` actuellement implemente dans `lpe-jmap` est aligne sur cette regle. `EmailSubmission/set` ne parle pas `SMTP`; il reutilise la soumission canonique existante apres lecture d'un brouillon persiste. `Mailbox/get`, `Email/query` et `Email/get` lisent la projection canonique sans reinjecter `Bcc` dans la recherche standard. Le scope supporte est detaille dans `docs/architecture/jmap-mail-mvp.md`.

Le MVP `IMAP` actuellement implemente dans `lpe-imap` suit la meme regle. `LOGIN`, `LIST`, `SELECT`, `FETCH`, `STORE`, `SEARCH` et `UID` lisent et modifient l'etat canonique des mailboxes, tandis que `APPEND` est limite a `Drafts` et reutilise la persistance canonique des brouillons sans logique parallele de `Sent`, `Drafts` ou `Outbox`. Le scope supporte est detaille dans `docs/architecture/imap-mvp.md`.

Le MVP `ActiveSync` actuellement implemente dans `lpe-activesync` est aligne sur la meme regle. `Provision`, `FolderSync`, `Sync` et `SendMail` sont implementes comme un adaptateur au-dessus de la meme authentification compte, de la meme persistance des brouillons, de la meme synchronisation mailbox et du meme modele canonique de soumission. `SendMail` ne contourne ni le workflow mailbox du coeur `LPE`, ni `LPE-CT`; il reutilise la soumission canonique pour que la copie autoritative `Sent` existe avant le relais sortant. Le scope supporte est detaille dans `docs/architecture/activesync-mvp.md`.

L'autoconfiguration client publie des endpoints reels seulement. En v1, `Thunderbird` peut recevoir l'acces `IMAP` et n'annonce un endpoint `SMTP` de soumission que si un endpoint client authentifie est explicitement publie; le relais interne `LPE -> LPE-CT` ne doit jamais etre annonce comme soumission client. Pour `Outlook`, l'autodiscover minimal doit pointer vers l'endpoint `ActiveSync` reel et ne doit pas annoncer `EWS`.

Le MVP `DAV` actuellement implemente dans `lpe-dav` suit la meme approche d'adaptateur pour les usages contacts/calendrier. `CardDAV` et `CalDAV` reutilisent la meme authentification compte, exposent `contacts` et `calendar_events` via un mapping DAV minimal, puis mettent a jour directement ces tables canoniques sans logique metier specifique a DAV. Le scope supporte est detaille dans `docs/architecture/dav-mvp.md`.

Le webmail utilise une authentification de compte distincte de l'administration. Le formulaire `/mail/` appelle `/api/mail/auth/login`, qui verifie le hash `argon2` stocke dans `account_credentials`, cree une session dans `account_sessions`, puis expose l'identite via `/api/mail/auth/me`.

Le webmail ne doit pas afficher de jeux de donnees de maquette en environnement fonctionnel. Apres authentification, il charge l'etat utilisateur par `/api/mail/workspace`, qui expose les messages, contacts et evenements persistants du compte. L'envoi, les brouillons, les contacts et le calendrier passent par des endpoints authentifies afin de rester alignes avec le modele canonique `LPE`. Les brouillons sont des messages persistants de la mailbox `Drafts`; leur edition met a jour la meme entree, leur expedition cree la copie autoritative `Sent` puis supprime la copie `Drafts`, et leur suppression ne peut viser qu'un message appartenant a la mailbox `Drafts` du compte authentifie.

### Blocs principaux

1. `lpe-domain`
Types metier partages.

2. `lpe-core`
Regles applicatives et orchestration du domaine.

3. `lpe-storage`
Adaptateur de persistance `PostgreSQL`, stockage blobs plus tard, et helpers partages de parsing mail (`RFC822`, en-tetes, adresses) reutilises par les couches d'import et les adaptateurs protocolaires quand ils doivent reconstruire le modele canonique sans dupliquer la logique.

4. `lpe-ai`
Contrats et services pour une IA locale future avec provenance.

5. `lpe-jmap`
Point d'entree moderne pour le client web et les futures apps natives.

6. `lpe-admin-api`
Plan de controle pour le back office.

7. `lpe-cli`
Executable de demarrage local du serveur.

8. `nginx` sur Debian
Serveur frontal HTTP pour exposer l'interface d'administration statique et reverse-proxy `/api/` vers `lpe-admin-api`.

9. `LPE-CT` pour la DMZ
Centre de tri distinct pour l'entree `SMTP` exposee, le relais sortant, le filtrage perimetrique, la quarantaine et le relais controle vers le coeur `LPE` dans le `LAN`.

### Priorites MVP

- comptes, domaines, alias, quotas
- IMAP
- transport `SMTP` entrant et sortant via `LPE-CT`
- compatibilite Outlook/mobile native via `ActiveSync` en premiere cible
- compatibilite contacts/calendrier via `CardDAV` et `CalDAV`
- `EWS` comme extension future apres stabilisation du modele canonique de soumission et de synchronisation
- coherence des messages envoyes entre protocoles clients et vue `Sent`
- webmail HTTPS
- recherche
- administration web
- projections documentaires et artefacts IA locaux
- indexation des pieces jointes `PDF`, `DOCX` et `ODT`
- interfaces web multilingues `en`, `fr`, `de`, `it`, `es`

## English

### Starting decisions

- primary store: `PostgreSQL`
- modern protocol axis: `JMAP`
- initial compatibility: `IMAP`; exposed `SMTP` transport handled by the `LPE-CT` sorting center
- LPE code: `Apache-2.0`
- dependencies: prefer `Apache-2.0`, allow `MIT` only with a documented exception
- data architecture prepared for future local AI
- distinct `LPE-CT` sorting center for exposed inbound and outbound `SMTP` transport, deployable in the `DMZ` without moving the business core

### Vision

`LPE` is a modern mail and collaboration server. The business core must not depend on `IMAP` or `SMTP`. External protocols are adapters around a stable internal model.

Native clients remain an important goal. A user must be able to connect an `LPE` mailbox from a compatible mobile or desktop client, for example the iPhone Mail application, without losing mailbox consistency across protocols.

This implies that every supported client submission path, especially `JMAP`, `IMAP`, or `ActiveSync`, feeds the same canonical message representation in `LPE`, including the authoritative `Sent` mailbox view. Inbound and outbound `SMTP` transport execution remains a sorting-center responsibility.

`ActiveSync` is the first targeted native Outlook and mobile compatibility layer. `EWS` remains a future extension to evaluate after the canonical submission and synchronization model is stabilized.

`CalDAV` and `CardDAV` are standards-based compatibility adapters for collaboration data. They must remain layered over the canonical `LPE` contact and calendar models, without introducing a separate DAV storage or rights model.

The initial submission model is transactional in the `LPE` core and exposed by `/api/mail/messages/submit`:

1. verify the submitting account
2. ensure the `Sent` mailbox exists
3. create the canonical message in `messages`
4. store visible recipients in `message_recipients` and retain `Bcc` in separate protected storage
5. index the body in `message_bodies` without including `Bcc` in `participants_normalized`
6. add an `outbound_message_queue` entry for handoff through `LPE-CT`
7. record the action in `audit_events`

This sequence makes `Sent` authoritative before the sorting center performs the actual SMTP delivery.

The functional v1 integration between the core platform and the sorting center is now explicit:

- an `LPE` worker reads `outbound_message_queue` and calls `LPE-CT`
- `LPE-CT` returns one of `queued`, `relayed`, `deferred`, `quarantined`, or `failed`
- `LPE-CT` delivers accepted inbound messages into `LPE` through an internal final-delivery API

The detailed contract is documented in `docs/architecture/lpe-ct-integration.md`.

All client layers must use this canonical submission and synchronization model. No client layer may write its own parallel `Sent` or `Outbox` logic.

Every file entering through an external connection or through a client must be validated with Google `Magika` before normal processing. This applies to `LPE-CT` for external ingress paths and to `LPE` for client-side uploads and imports.

The currently implemented `JMAP Mail` MVP in `lpe-jmap` follows that rule. `EmailSubmission/set` does not speak `SMTP`; it reuses the existing canonical submission workflow after loading a persisted draft. `Mailbox/get`, `Email/query`, and `Email/get` read the canonical mailbox projection without reinjecting `Bcc` into standard search paths. The supported scope is detailed in `docs/architecture/jmap-mail-mvp.md`.

The currently implemented `IMAP` MVP in `lpe-imap` follows the same rule. `LOGIN`, `LIST`, `SELECT`, `FETCH`, `STORE`, `SEARCH`, and `UID` read and update canonical mailbox state, while `APPEND` is limited to `Drafts` and reuses canonical draft persistence without introducing parallel `Sent`, `Drafts`, or `Outbox` logic. The supported scope is detailed in `docs/architecture/imap-mvp.md`.

The current `ActiveSync` MVP in `lpe-activesync` follows the same rule. `Provision`, `FolderSync`, `Sync`, and `SendMail` are implemented as an adapter over the same account authentication, draft persistence, mailbox synchronization, and canonical submission model. `SendMail` does not bypass the core mailbox workflow or `LPE-CT`; it reuses the canonical submission path so the authoritative `Sent` copy exists before outbound relay. The supported scope is detailed in `docs/architecture/activesync-mvp.md`.

Client auto-configuration must publish only real endpoints. In v1, `Thunderbird` may receive `IMAP` settings and must advertise `SMTP` submission only when an authenticated client-submission endpoint is explicitly exposed; the internal `LPE -> LPE-CT` relay must never be described as a client-submission service. For `Outlook`, minimal autodiscovery must point to the real `ActiveSync` endpoint and must not advertise `EWS`.

The current `DAV` MVP in `lpe-dav` follows the same adapter approach for collaboration compatibility. `CardDAV` and `CalDAV` reuse the same mailbox-account authentication, expose `contacts` and `calendar_events` through a minimal DAV collection model, and update those canonical tables directly instead of introducing DAV-only business logic. The supported scope is detailed in `docs/architecture/dav-mvp.md`.

The webmail uses account authentication separate from administration. The `/mail/` form calls `/api/mail/auth/login`, which verifies the `argon2` hash stored in `account_credentials`, creates a session in `account_sessions`, and exposes the identity through `/api/mail/auth/me`.

The webmail must not display mock datasets in a functional environment. After authentication, it loads user state through `/api/mail/workspace`, which exposes persistent messages, contacts, and events for the account. Submission, drafts, contacts, and calendar entries go through authenticated endpoints so the client remains aligned with the canonical `LPE` model. Drafts are persistent messages in the `Drafts` mailbox; editing updates the same row, sending creates the authoritative `Sent` copy and then removes the `Drafts` copy, and deletion is limited to a message owned by the authenticated account in the `Drafts` mailbox.

### Main building blocks

1. `lpe-domain`
Shared business types.

2. `lpe-core`
Application rules and domain orchestration.

3. `lpe-storage`
`PostgreSQL` persistence adapter, later blob storage, and shared mail parsing helpers (`RFC822`, headers, addresses) reused by import paths and protocol adapters when they need to rebuild the canonical model without duplicating parsing logic.

4. `lpe-ai`
Contracts and services for future local AI with provenance.

5. `lpe-jmap`
Modern entry point for the web client and future native apps.

6. `lpe-admin-api`
Control plane for the back office.

7. `lpe-cli`
Local server executable.

8. `nginx` on Debian
HTTP front end used to expose the static administration UI and reverse-proxy `/api/` to `lpe-admin-api`.

9. `LPE-CT` in the DMZ
Separate sorting center for exposed `SMTP` ingress, outbound relay, perimeter filtering, quarantine, and controlled relay toward the core `LPE` services on the `LAN`.

### MVP priorities

- accounts, domains, aliases, quotas
- IMAP
- inbound and outbound `SMTP` transport through `LPE-CT`
- native Outlook and mobile compatibility through `ActiveSync` as the first target
- contacts and calendar compatibility through `CardDAV` and `CalDAV`
- `EWS` as a future extension after stabilization of the canonical submission and synchronization model
- sent-message consistency across client protocols and the `Sent` view
- HTTPS webmail
- search
- web administration
- document projections and local AI artifacts
- attachment indexing for `PDF`, `DOCX`, and `ODT`
- multilingual web interfaces for `en`, `fr`, `de`, `it`, `es`
