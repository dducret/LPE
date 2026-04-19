# Contrat d'integration `LPE` / `LPE-CT` | `LPE` / `LPE-CT` integration contract

## Francais

### Objectif

Ce document fixe le contrat explicite entre le coeur `LPE` et le centre de tri `LPE-CT`.

La separation d'architecture reste stricte:

- `LPE` reste systeme de record des mailbox, des vues utilisateur et de `Sent`
- `LPE-CT` reste responsable du `SMTP` expose, du relais sortant, de la quarantaine et du perimetre
- aucun `SMTP` Internet-facing n'est reintroduit dans `LPE`

### Flux sortant `LPE -> LPE-CT`

1. un client soumet un message via le modele canonique `LPE`
2. `LPE` persiste la copie autoritative dans `Sent`
3. `LPE` ajoute une entree `outbound_message_queue`
4. le worker sortant `LPE` consomme les elements `queued` ou `deferred`
5. le worker appelle `POST /api/v1/integration/outbound-messages` sur `LPE-CT`
6. `LPE-CT` applique ses politiques locales puis tente le relais `SMTP`
7. `LPE-CT` repond avec un statut explicite et un `trace_id`
8. `LPE` met a jour `outbound_message_queue.status`, `messages.delivery_status`, `remote_message_ref`, `attempts`, `last_error` et `next_attempt_at`

Statuts minimaux supportes:

- `queued`: message prepare dans `LPE` avant handoff
- `relayed`: `LPE-CT` a accepte le handoff et relaye le message vers la cible `SMTP`
- `deferred`: echec transitoire; `LPE` recalcule `next_attempt_at`
- `quarantined`: `LPE-CT` a retenu le message selon sa politique
- `failed`: echec permanent ou configuration incompatible avec le relais

### Flux entrant `LPE-CT -> LPE`

1. `LPE-CT` recoit le message `SMTP` depuis Internet
2. `LPE-CT` applique drainage, quarantaine et controles de perimeterie
3. si le message est accepte pour livraison finale, `LPE-CT` appelle `POST /internal/lpe-ct/inbound-deliveries` sur `LPE`
4. `LPE` verifie la cle d'integration, resout les mailbox locales par destinataire, cree la copie canonique dans `Inbox`, indexe le corps et journalise l'action
5. `LPE` retourne les destinataires acceptes et rejetes
6. `LPE-CT` met a jour son spool local en `sent`, `deferred` ou `held`

Le corps `SMTP` brut est transporte vers `LPE` pour conserver le contexte de livraison, mais la persistance mailbox reste controlee par le modele interne `LPE`.

### Authentification interne

Les deux appels HTTP utilisent l'en-tete:

- `x-lpe-integration-key`

La cle partagee est fournie par:

- `LPE_INTEGRATION_SHARED_SECRET`

Cette cle est obligatoire au demarrage des deux composants, doit echouer fermee si elle manque, et ne doit pas utiliser de valeur triviale ou connue telle que `change-me`. Une longueur minimale de `32` caracteres est requise. Cette cle ne doit pas etre exposee dans les interfaces publiques ni stockee dans l'etat de management JSON de `LPE-CT`.

### Variables d'environnement principales

Cote `LPE`:

- `LPE_CT_API_BASE_URL`
- `LPE_OUTBOUND_WORKER_INTERVAL_MS`
- `LPE_OUTBOUND_WORKER_BATCH_SIZE`
- `LPE_INTEGRATION_SHARED_SECRET`

Cote `LPE-CT`:

- `LPE_CT_CORE_DELIVERY_BASE_URL`
- `LPE_CT_RELAY_PRIMARY`
- `LPE_CT_RELAY_SECONDARY`
- `LPE_INTEGRATION_SHARED_SECRET`

### Remarques d'implementation v1

- le worker `LPE` effectue un handoff synchrone et met a jour l'etat ensuite
- `LPE-CT` conserve les octets SMTP bruts sur l'entree et les transporte jusqu'a la persistance et a la remise finale interne
- `LPE-CT` extrait le texte visible entrant a partir du MIME decode (`multipart/alternative`, `quoted-printable`, `base64`, HTML) sans indexer brutement tout le body RFC 822
- `LPE-CT` compose le relais sortant en RFC 822 avec `text/plain` seul ou `multipart/alternative` `text/plain` + `text/html` quand `body_html_sanitized` est disponible, sans reinjecter `Bcc` dans les en-tetes visibles
- la remise finale entrante cree des copies `Inbox` par mailbox resolue dans `LPE`
- la recherche standard et les projections visibles ne reinjectent pas `Bcc`

## English

### Goal

This document defines the explicit contract between the core `LPE` services and the `LPE-CT` sorting center.

The architectural split remains strict:

- `LPE` remains the system of record for mailboxes, user-visible views, and `Sent`
- `LPE-CT` remains responsible for exposed `SMTP`, outbound relay, quarantine, and the edge perimeter
- no Internet-facing `SMTP` is reintroduced into `LPE`

### Outbound flow `LPE -> LPE-CT`

1. a client submits a message through the canonical `LPE` model
2. `LPE` persists the authoritative copy in `Sent`
3. `LPE` inserts an `outbound_message_queue` row
4. the `LPE` outbound worker consumes items in `queued` or `deferred`
5. the worker calls `POST /api/v1/integration/outbound-messages` on `LPE-CT`
6. `LPE-CT` applies local policy and attempts `SMTP` relay
7. `LPE-CT` replies with an explicit status and a `trace_id`
8. `LPE` updates `outbound_message_queue.status`, `messages.delivery_status`, `remote_message_ref`, `attempts`, `last_error`, and `next_attempt_at`

Minimum supported statuses:

- `queued`: message prepared in `LPE` before handoff
- `relayed`: `LPE-CT` accepted the handoff and relayed the message toward its `SMTP` target
- `deferred`: transient failure; `LPE` recomputes `next_attempt_at`
- `quarantined`: `LPE-CT` retained the message under policy
- `failed`: permanent failure or relay-incompatible configuration

### Inbound flow `LPE-CT -> LPE`

1. `LPE-CT` receives the `SMTP` message from the Internet
2. `LPE-CT` applies drain, quarantine, and edge controls
3. if the message is accepted for final delivery, `LPE-CT` calls `POST /internal/lpe-ct/inbound-deliveries` on `LPE`
4. `LPE` verifies the integration key, resolves local mailboxes by recipient, creates the canonical `Inbox` copy, indexes the body, and records the action
5. `LPE` returns accepted and rejected recipients
6. `LPE-CT` updates its local spool into `sent`, `deferred`, or `held`

The raw `SMTP` body is carried into `LPE` to keep delivery context, but mailbox persistence remains controlled by the internal `LPE` model.

### Internal authentication

Both HTTP calls use the header:

- `x-lpe-integration-key`

The shared secret is provided through:

- `LPE_INTEGRATION_SHARED_SECRET`

That secret is mandatory at startup on both components, must fail closed when missing, and must not use a trivial or known placeholder value such as `change-me`. A minimum length of `32` characters is required. That secret must not be exposed in public interfaces or stored in the `LPE-CT` management JSON state.

### Main environment variables

On the `LPE` side:

- `LPE_CT_API_BASE_URL`
- `LPE_OUTBOUND_WORKER_INTERVAL_MS`
- `LPE_OUTBOUND_WORKER_BATCH_SIZE`
- `LPE_INTEGRATION_SHARED_SECRET`

On the `LPE-CT` side:

- `LPE_CT_CORE_DELIVERY_BASE_URL`
- `LPE_CT_RELAY_PRIMARY`
- `LPE_CT_RELAY_SECONDARY`
- `LPE_INTEGRATION_SHARED_SECRET`

### v1 implementation notes

- the `LPE` worker performs synchronous handoff and updates state afterward
- `LPE-CT` keeps raw SMTP bytes intact on ingress and carries them through persistence and internal final delivery
- `LPE-CT` extracts inbound visible text from decoded MIME (`multipart/alternative`, `quoted-printable`, `base64`, HTML) instead of indexing the raw RFC 822 body blindly
- `LPE-CT` composes outbound relay as RFC 822 with either plain `text/plain` or `multipart/alternative` `text/plain` + `text/html` when `body_html_sanitized` is available, without reinjecting `Bcc` into visible headers
- inbound final delivery creates per-mailbox `Inbox` copies in `LPE`
- standard search and visible projections do not reinject `Bcc`
