# Sieve / ManageSieve MVP

## Francais

### Objectif

Ce document decrit le premier support `Sieve` et `ManageSieve` dans `LPE`.

Le but est de fournir un filtrage mailbox par compte sans reintroduire une logique metier parallele hors du modele canonique `LPE`.

### Positionnement architectural

- `Sieve` est une regle mailbox utilisateur, pas une politique de bord `LPE-CT`
- `ManageSieve` est un adaptateur protocolaire de gestion de scripts, pas un nouveau modele de workflow
- la livraison finale entrante reste le point canonique d'execution
- les sorties automatiques `redirect` et `vacation` reutilisent la soumission canonique `LPE` et la file `outbound_message_queue`

### Stockage canonique

- scripts stockes en base `PostgreSQL` par `(tenant_id, account_id)`
- un seul script actif par compte
- memoire minimale `vacation` stockee par compte et expediteur pour limiter les reponses repetees
- audit minimal sur creation, mise a jour, renommage, activation, suppression et application des scripts

### Scope MVP

Le MVP supporte:

- stockage de scripts par compte
- service `ManageSieve` minimal authentifie par le meme login de compte que les autres protocoles mailbox
- script actif par compte
- execution pendant la remise entrante cote `LPE`
- actions `fileinto`, `discard`, `redirect`, `vacation`, `keep`, `stop`
- tests `header`, `address`, `envelope`, `allof`, `anyof`, `not`, `true`, `false`
- creation automatique d'un dossier cible `fileinto` s'il n'existe pas encore pour le compte

### Invariants

- la resolution multi-tenant reste faite par destinataire accepte
- `Sieve` n'est execute qu'apres resolution du compte destinataire
- `Inbox`, `Sent` et `Drafts` restent des vues canoniques
- `fileinto` change la mailbox de livraison locale, sans creer de copie parallele
- `discard` supprime la copie locale tout en laissant la remise consideree comme acceptee pour le destinataire
- `redirect` et `vacation` n'envoient rien directement vers Internet; ils reutilisent `submit_message` et `LPE-CT`

### Protections MVP

- taille de script limitee a `64 KiB`
- maximum `16` scripts par compte
- maximum `4` redirects par message
- `vacation` applique une memoire minimale par expediteur et contenu de reponse
- `ManageSieve` MVP accepte uniquement `AUTHENTICATE PLAIN`
- `ManageSieve` MVP accepte uniquement les literals non synchronisants `{N+}`

### Limitations explicites

- le sous-ensemble `Sieve` est volontairement borne; il ne couvre pas l'ensemble de `RFC 5228`
- pas de support `include`, `variables`, `imapflags`, `reject`, `ereject`, `date`, `relational`, `body`, `spamtest`, ni extensions vendor-specific
- pas de `:copy` en `fileinto` ni de multi-script actif
- `redirect` MVP re-soumet un message canonique reconstruit par `LPE`; il ne garantit pas une re-emission byte-identique du flux entrant initial
- `redirect` et `vacation` creent aujourd'hui une copie canonique sortante dans `Sent` avec une source technique `sieve-*`; ce point est accepte pour le MVP afin d'eviter une logique sortante parallele
- pas d'interface web d'administration dediee des scripts dans ce premier jet

## English

### Goal

This document describes the first `Sieve` and `ManageSieve` support in `LPE`.

The goal is to provide per-account mailbox filtering without reintroducing parallel business logic outside the canonical `LPE` model.

### Architectural placement

- `Sieve` is an end-user mailbox rule, not an `LPE-CT` edge policy
- `ManageSieve` is a script-management protocol adapter, not a new workflow model
- final inbound delivery remains the canonical execution point
- automatic outbound effects from `redirect` and `vacation` reuse canonical `LPE` submission and `outbound_message_queue`

### Canonical storage

- scripts are stored in `PostgreSQL` per `(tenant_id, account_id)`
- only one active script is allowed per account
- minimal `vacation` memory is stored per account and sender to limit repeated replies
- minimal audit coverage exists for script creation, update, rename, activation, deletion, and application

### MVP scope

The MVP supports:

- per-account script storage
- a minimal `ManageSieve` service authenticated with the same mailbox-account login as the other mailbox protocols
- one active script per account
- execution during inbound final delivery on the `LPE` side
- `fileinto`, `discard`, `redirect`, `vacation`, `keep`, and `stop`
- `header`, `address`, `envelope`, `allof`, `anyof`, `not`, `true`, and `false` tests
- automatic creation of the target `fileinto` mailbox when it does not already exist for the account

### Invariants

- multi-tenant resolution still happens per accepted recipient
- `Sieve` is executed only after the target account is resolved
- `Inbox`, `Sent`, and `Drafts` remain canonical views
- `fileinto` changes local delivery placement without creating a parallel copy
- `discard` removes the local copy while keeping the recipient delivery accepted
- `redirect` and `vacation` do not send anything directly to the Internet; they reuse `submit_message` and `LPE-CT`

### MVP protections

- script size is limited to `64 KiB`
- at most `16` scripts per account
- at most `4` redirects per message
- `vacation` applies minimal memory keyed by sender and response content
- the `ManageSieve` MVP supports only `AUTHENTICATE PLAIN`
- the `ManageSieve` MVP supports only non-synchronizing literals `{N+}`

### Explicit limitations

- the supported `Sieve` subset is intentionally bounded and does not cover the full `RFC 5228` surface
- there is no support for `include`, `variables`, `imapflags`, `reject`, `ereject`, `date`, `relational`, `body`, `spamtest`, or vendor-specific extensions
- there is no `:copy` support for `fileinto` and no multi-active-script mode
- the MVP `redirect` path re-submits an `LPE` canonical message and does not guarantee byte-identical replay of the original inbound stream
- `redirect` and `vacation` currently create a canonical outbound copy in `Sent` with a `sieve-*` technical source; this is accepted for the MVP in order to avoid parallel outbound logic
- there is no dedicated web administration UI for scripts in this first iteration
