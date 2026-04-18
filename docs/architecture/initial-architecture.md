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

Le modele de soumission initial est transactionnel cote coeur `LPE` et expose par l'API `/api/mail/messages/submit`:

1. verifier le compte emetteur
2. garantir l'existence de la mailbox `Sent`
3. creer le message canonique dans `messages`
4. enregistrer les destinataires dans `message_recipients`
5. indexer le corps dans `message_bodies`
6. ajouter une entree `outbound_message_queue` pour remise via `LPE-CT`
7. journaliser l'action dans `audit_events`

Cette sequence donne a `Sent` le statut de source autoritative avant meme la remise SMTP effective par le centre de tri.

Toutes les couches clientes doivent utiliser ce modele canonique de soumission et de synchronisation. Aucune couche cliente ne doit ecrire une logique `Sent` ou `Outbox` parallele.

Le webmail utilise une authentification de compte distincte de l'administration. Le formulaire `/mail/` appelle `/api/mail/auth/login`, qui verifie le hash `argon2` stocke dans `account_credentials`, cree une session dans `account_sessions`, puis expose l'identite via `/api/mail/auth/me`.

Le webmail ne doit pas afficher de jeux de donnees de maquette en environnement fonctionnel. Apres authentification, il charge l'etat utilisateur par `/api/mail/workspace`, qui expose les messages, contacts et evenements persistants du compte. L'envoi, les brouillons, les contacts et le calendrier passent par des endpoints authentifies afin de rester alignes avec le modele canonique `LPE`.

### Blocs principaux

1. `lpe-domain`
Types metier partages.

2. `lpe-core`
Regles applicatives et orchestration du domaine.

3. `lpe-storage`
Adaptateur de persistance `PostgreSQL` et stockage blobs plus tard.

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

The initial submission model is transactional in the `LPE` core and exposed by `/api/mail/messages/submit`:

1. verify the submitting account
2. ensure the `Sent` mailbox exists
3. create the canonical message in `messages`
4. store recipients in `message_recipients`
5. index the body in `message_bodies`
6. add an `outbound_message_queue` entry for handoff through `LPE-CT`
7. record the action in `audit_events`

This sequence makes `Sent` authoritative before the sorting center performs the actual SMTP delivery.

All client layers must use this canonical submission and synchronization model. No client layer may write its own parallel `Sent` or `Outbox` logic.

The webmail uses account authentication separate from administration. The `/mail/` form calls `/api/mail/auth/login`, which verifies the `argon2` hash stored in `account_credentials`, creates a session in `account_sessions`, and exposes the identity through `/api/mail/auth/me`.

The webmail must not display mock datasets in a functional environment. After authentication, it loads user state through `/api/mail/workspace`, which exposes persistent messages, contacts, and events for the account. Submission, drafts, contacts, and calendar entries go through authenticated endpoints so the client remains aligned with the canonical `LPE` model.

### Main building blocks

1. `lpe-domain`
Shared business types.

2. `lpe-core`
Application rules and domain orchestration.

3. `lpe-storage`
`PostgreSQL` persistence adapter and later blob storage.

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
- `EWS` as a future extension after stabilization of the canonical submission and synchronization model
- sent-message consistency across client protocols and the `Sent` view
- HTTPS webmail
- search
- web administration
- document projections and local AI artifacts
- attachment indexing for `PDF`, `DOCX`, and `ODT`
- multilingual web interfaces for `en`, `fr`, `de`, `it`, `es`
