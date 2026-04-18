# LLM local-first

## Francais

### Objectif

`LPE` doit pouvoir integrer un moteur LLM local plus tard, par exemple une famille type Gemma, sans faire sortir les donnees du serveur et sans rendre l'IA indispensable au coeur produit.

### Principes

- toutes les donnees utilisateur restent sur l'infrastructure `LPE`
- le moteur IA accede a des projections documentaires normalisees, pas au MIME brut comme source primaire
- toutes les requetes IA sont filtrees par ACL avant inference
- les metadonnees protegees comme `Bcc` sont exclues des projections, chunks et entrees d'inference utilisateur; elles restent stockees separement des destinataires visibles et hors index standard
- chaque resultat IA conserve une provenance exploitable
- la recherche PostgreSQL reste prioritaire pour la precision, la vitesse et les filtres

### Strategie de donnees

Chaque objet metier important doit pouvoir produire une projection canonique:

- message
- evenement calendrier
- contact
- piece jointe

Chaque projection doit fournir:

- identifiant stable
- type source
- texte normalise
- langue
- participants normalises
- empreinte de contenu
- proprietaire et empreinte ACL

### Pipeline

1. ingestion du message ou objet metier
2. normalisation de contenu
3. indexation `PostgreSQL` full-text
4. creation optionnelle de chunks
5. enrichissements IA locaux asynchrones
6. stockage des artefacts IA avec provenance

### Artefacts prevus

- `document_projections`
- `document_chunks`
- `document_annotations`
- `inference_runs`
- `inference_run_chunks`

### Interface modele

Le backend expose un contrat generique de fournisseur local:

- decrire les modeles locaux disponibles
- executer une inference
- retourner la sortie et la provenance des chunks exploites

Le coeur produit ne doit pas dependre d'un modele donne. `Gemma` doit etre un futur fournisseur local parmi d'autres.

## English

### Goal

`LPE` must be able to integrate a local LLM later, for example a Gemma-like family, without sending data outside the server and without making AI mandatory for the product core.

### Principles

- all user data stays on `LPE` infrastructure
- the AI engine reads normalized document projections, not raw MIME as the primary source
- all AI requests are ACL-filtered before inference
- protected metadata such as `Bcc` is excluded from user-facing projections, chunks, and inference inputs; it remains stored separately from visible recipients and outside default search indexes
- every AI result keeps usable provenance
- PostgreSQL search remains the primary layer for precision, speed, and filtering

### Data strategy

Each important business object must be able to produce a canonical projection:

- message
- calendar event
- contact
- attachment

Each projection must provide:

- stable identifier
- source type
- normalized text
- language
- normalized participants
- content fingerprint
- owner and ACL fingerprint

### Pipeline

1. ingest the message or business object
2. normalize content
3. index with PostgreSQL full-text search
4. optionally create chunks
5. run asynchronous local AI enrichments
6. store AI artifacts with provenance

### Planned artifacts

- `document_projections`
- `document_chunks`
- `document_annotations`
- `inference_runs`
- `inference_run_chunks`

### Model interface

The backend exposes a generic local provider contract:

- describe available local models
- execute an inference
- return output and provenance for the chunks used

The product core must not depend on a single model. `Gemma` should be one future local provider among others.

