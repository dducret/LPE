# Client autoconfiguration | Client autoconfiguration

## Francais

### Objectif

Ce document decrit les endpoints d'autoconfiguration client publies par `LPE` pour le MVP.

Le principe directeur est strict: publier seulement ce qui est effectivement implemente et expose.

### Endpoints publies

- `GET /autoconfig/mail/config-v1.1.xml`
- `GET /.well-known/autoconfig/mail/config-v1.1.xml`
- `GET /autodiscover/autodiscover.xml`
- `POST /autodiscover/autodiscover.xml`
- `GET /Autodiscover/Autodiscover.xml`
- `POST /Autodiscover/Autodiscover.xml`

Sans reverse proxy, ces routes sont exposees directement par le service Rust `LPE`.

Avec le reverse proxy Debian documente dans le projet, ces routes sont publiees telles quelles par `nginx` et doivent ensuite etre re-exposees par `LPE-CT` sur le hostname public client.

### Thunderbird

L'autoconfig `Thunderbird` publie:

- `IMAP` vers le host public configure
- port `993` par defaut
- `SSL` implicite
- authentification `password-cleartext` sur tunnel TLS
- username `%EMAILADDRESS%`

Par defaut, aucun endpoint `SMTP` de soumission client n'est annonce.

Cette absence est volontaire: le depot publie aujourd'hui le relais interne `LPE -> LPE-CT` et un listener `SMTP` perimetrique minimal cote `LPE-CT`, mais pas encore un endpoint de soumission client authentifie de type `465` ou `587`.

Un bloc `SMTP` n'est inclus dans l'XML que si un endpoint de soumission client reel est configure explicitement via l'environnement.

### Outlook

L'autodiscover `Outlook` minimal publie uniquement:

- `ActiveSync`
- URL `https://<host-public>/Microsoft-Server-ActiveSync`

Le MVP n'annonce pas `EWS`.

Ce choix est coherent avec l'architecture `LPE`: la compatibilite native `Outlook` et mobile prioritaire passe d'abord par `ActiveSync`.

### JMAP

`JMAP` reste le protocole moderne principal, mais le MVP d'autoconfiguration client ajoute seulement une reference documentaire vers la session `JMAP` publiee sur:

- `GET /api/jmap/session`

Le MVP ne publie pas encore de well-known `JMAP` dedie.

### Variables d'environnement

- `LPE_PUBLIC_SCHEME`, par defaut `https`
- `LPE_PUBLIC_HOSTNAME`, optionnel; par defaut le host public est deduit de `Host` ou `X-Forwarded-Host`
- `LPE_AUTOCONFIG_IMAP_HOST`, optionnel
- `LPE_AUTOCONFIG_IMAP_PORT`, par defaut `993`
- `LPE_AUTOCONFIG_SMTP_HOST`, optionnel; active la publication d'un bloc `SMTP`
- `LPE_AUTOCONFIG_SMTP_PORT`, par defaut `465`
- `LPE_AUTOCONFIG_SMTP_SOCKET_TYPE`, par defaut `SSL`
- `LPE_AUTODISCOVER_ACTIVESYNC_URL`, optionnel
- `LPE_AUTOCONFIG_JMAP_SESSION_URL`, optionnel

### DNS et HTTP recommandes

Pour un domaine `example.test`:

- publier `autoconfig.example.test` ou `mail.example.test` vers le frontal public `LPE-CT`
- publier `autodiscover.example.test` ou reutiliser `mail.example.test` vers le meme frontal
- re-exposer en HTTPS les routes `/autoconfig/...`, `/.well-known/autoconfig/...`, `/autodiscover/...`, `/Autodiscover/...` et `/Microsoft-Server-ActiveSync`
- publier `IMAPS` vers le meme hostname si l'acces `IMAP` natif est expose
- ne pas reutiliser le relais interne `LPE -> LPE-CT` comme endpoint de soumission client

## English

### Goal

This document describes the client auto-configuration endpoints published by `LPE` for the MVP.

The guiding principle is strict: publish only what is actually implemented and exposed.

### Published endpoints

- `GET /autoconfig/mail/config-v1.1.xml`
- `GET /.well-known/autoconfig/mail/config-v1.1.xml`
- `GET /autodiscover/autodiscover.xml`
- `POST /autodiscover/autodiscover.xml`
- `GET /Autodiscover/Autodiscover.xml`
- `POST /Autodiscover/Autodiscover.xml`

Without a reverse proxy, these routes are exposed directly by the Rust `LPE` service.

With the documented Debian reverse proxy, those routes are published as-is by `nginx` and should then be re-exposed by `LPE-CT` on the public client hostname.

### Thunderbird

Thunderbird autoconfig publishes:

- `IMAP` against the configured public host
- port `993` by default
- implicit `SSL`
- `password-cleartext` authentication inside TLS
- username `%EMAILADDRESS%`

By default, no client `SMTP` submission endpoint is advertised.

That omission is intentional: the repository currently publishes the internal `LPE -> LPE-CT` relay and a minimal perimeter `SMTP` listener on the `LPE-CT` side, but it does not yet expose an authenticated client-submission endpoint such as `465` or `587`.

An `SMTP` block is included in the XML only when a real client-submission endpoint is explicitly configured through the environment.

### Outlook

Minimal Outlook autodiscovery publishes only:

- `ActiveSync`
- URL `https://<public-host>/Microsoft-Server-ActiveSync`

The MVP does not advertise `EWS`.

That choice stays aligned with the `LPE` architecture: the first priority for native Outlook and mobile compatibility is `ActiveSync`.

### JMAP

`JMAP` remains the primary modern protocol, but the MVP client-autoconfiguration layer only adds a documentation pointer to the published `JMAP` session endpoint:

- `GET /api/jmap/session`

The MVP does not yet publish a dedicated `JMAP` well-known endpoint.

### Environment variables

- `LPE_PUBLIC_SCHEME`, default `https`
- `LPE_PUBLIC_HOSTNAME`, optional; by default the public host is inferred from `Host` or `X-Forwarded-Host`
- `LPE_AUTOCONFIG_IMAP_HOST`, optional
- `LPE_AUTOCONFIG_IMAP_PORT`, default `993`
- `LPE_AUTOCONFIG_SMTP_HOST`, optional; enables the published `SMTP` block
- `LPE_AUTOCONFIG_SMTP_PORT`, default `465`
- `LPE_AUTOCONFIG_SMTP_SOCKET_TYPE`, default `SSL`
- `LPE_AUTODISCOVER_ACTIVESYNC_URL`, optional
- `LPE_AUTOCONFIG_JMAP_SESSION_URL`, optional

### Recommended DNS and HTTP publication

For a domain `example.test`:

- publish `autoconfig.example.test` or `mail.example.test` toward the public `LPE-CT` front end
- publish `autodiscover.example.test` or reuse `mail.example.test` toward the same front end
- re-expose the `/autoconfig/...`, `/.well-known/autoconfig/...`, `/autodiscover/...`, `/Autodiscover/...`, and `/Microsoft-Server-ActiveSync` routes over HTTPS
- publish `IMAPS` on the same hostname when native `IMAP` access is exposed
- do not reuse the internal `LPE -> LPE-CT` relay as a client-submission endpoint
