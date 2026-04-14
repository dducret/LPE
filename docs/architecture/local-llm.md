# LLM local-first

## Objectif

`LPE` doit pouvoir integrer un moteur LLM local plus tard, par exemple une famille type Gemma, sans faire sortir les donnees du serveur et sans rendre l'IA indispensable au coeur produit.

## Principes

- toutes les donnees utilisateur restent sur l'infrastructure `LPE`
- le moteur IA accede a des projections documentaires normalisees, pas au MIME brut comme source primaire
- toutes les requetes IA sont filtrees par ACL avant inférence
- chaque resultat IA conserve une provenance exploitable
- la recherche PostgreSQL reste prioritaire pour la precision, la vitesse et les filtres

## Strategie de donnees

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

## Pipeline

1. ingestion du message ou objet metier
2. normalisation de contenu
3. indexation `PostgreSQL` full-text
4. creation optionnelle de chunks
5. enrichissements IA locaux asynchrones
6. stockage des artefacts IA avec provenance

## Artefacts prevus

- `document_projections`
- `document_chunks`
- `document_annotations`
- `inference_runs`
- `inference_run_chunks`

## Interface modele

Le backend expose un contrat generique de fournisseur local:

- decrire les modeles locaux disponibles
- executer une inférence
- retourner la sortie + la provenance des chunks exploites

Le coeur produit ne doit pas dependre d'un modele donne. `Gemma` doit etre un futur fournisseur local parmi d'autres.

