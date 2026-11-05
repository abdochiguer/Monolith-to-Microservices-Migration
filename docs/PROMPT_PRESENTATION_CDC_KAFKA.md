# PROMPT COMPLET POUR GÉNÉRER LA PRÉSENTATION
## Thème : Change Data Capture (CDC), Apache Kafka & Implémentation Réelle sur la Migration Monolithe vers Microservices

> **Instructions pour l'utilisateur** :  
> Copiez l'intégralité du texte ci-dessous et collez-le dans votre LLM (ex: ChatGPT, Claude, Gemini). Ce prompt est calibré pour structurer une présentation professionnelle slide par slide, avec explications théoriques, architecture technique et étude de cas détaillée sur votre projet réel.

---

```markdown
Tu es un architecte logiciel et expert en systèmes distribués (Data Streaming, Kafka, CDC, Kubernetes). 
Je dois préparer une présentation technique de haut niveau (environ 12 à 15 slides) pour un public de développeurs et d'architectes. 

Mon objectif est double :
1. Expliquer de manière claire, rigoureuse et pédagogique les concepts fondamentaux du Change Data Capture (CDC) et d'Apache Kafka (utilité, architecture interne, fonctionnement, comparaison avec les approches traditionnelles).
2. Présenter en détail l'implémentation concrète et réelle mise en place sur mon projet de migration d'un système monolithique vers les microservices sous Kubernetes.

Pour chaque slide, tu dois fournir :
- **Titre de la Slide**
- **Points clés (Bullet points percutants et clairs)**
- **Diagramme ASCII / Mermaid ou schéma visuel recommandé**
- **Notes de l'orateur (Speaker Notes) rédigées à la première personne**, très détaillées, pour que je sache exactement quoi dire à l'oral avec des explications simples et percutantes.

---

### CONTEXTE DU PROJET RÉEL À INTÉGRER DANS LA DEUXIÈME PARTIE :

- **Contexte** : Découpage d'un monolithe applicatif (Spring PetClinic) vers un microservice dédié aux vétérinaires (`vet-service`) hébergé sur Kubernetes (Minikube).
- **Problématique** : Séparation stricte de la base de données (*Database-per-Service*). L'ancienne base est `demo-db` (PostgreSQL), la nouvelle base cible est `vet-db` (PostgreSQL dans le namespace `microservices`).
- **Défi résolu** : Éviter le piège de la double écriture applicative (*Dual-Write*). L'application legacy continue de recevoir du trafic, mais toutes les créations/modifications de vétérinaires doivent être répliquées en continu et de manière asynchrone vers la nouvelle base `vet-db` sans modifier le code source du monolithe.
- **Pile technique réelle utilisée** :
  1. **PostgreSQL Source (`demo-db`)** : Configuré avec `wal_level=logical`, slots de réplication logique (`max_replication_slots=4`, `max_wal_senders=4`), plugin `pgoutput`.
  2. **Strimzi Kafka Operator sur Kubernetes** :
     - Cluster Kafka en mode **KRaft** (sans Zookeeper) avec `KafkaNodePool` (1 nœud, listener plain 9092).
     - Déploiement de `KafkaConnect` nommé `debezium-connect` avec une image personnalisée contenant le connecteur Postgres et le connecteur JDBC Sink.
  3. **Connecteur Source Debezium (`KafkaConnector : petclinic-postgres-source`)** :
     - Écoute les tables `public.vets`, `public.specialties`, `public.vet_specialties`.
     - Publication dans des topics Kafka avec le préfixe `petclinic-vet-cdc`.
     - Paramètre `snapshot.mode: no_data` (car les données historiques ont déjà été migrées via un Job K8s one-shot).
     - Slot de réplication : `debezium_petclinic` avec publication `debezium_pub`.
  4. **Connecteur Sink JDBC (`KafkaConnector : vet-vets-sink`)** :
     - Consomme le topic `petclinic-vet-cdc.public.vets`.
     - Écrit dans `vet-db` via JDBC (`jdbc:postgresql://vet-db.microservices.svc.cluster.local:5432/vetdb`).
     - Mode `insert.mode: upsert` basé sur la clé primaire `id` (`record_key`).
     - Sécurité : Identifiants injectés dynamiquement depuis un Secret Kubernetes via `KubernetesSecretConfigProvider`.
  5. **Contraintes et incidents réels rencontrés** :
     - Surcharge mémoire / JVM (`CrashLoopBackOff`) sur le cluster Minikube nécessitant un dimensionnement strict des requêtes/limites mémoires et JVM (`-Xms256m -Xmx384m` pour Kafka, `-Xms256m -Xmx512m` pour Connect).
     - Découplage de la phase d'historique (Job batch one-shot) et de la phase de réplication temps réel (`snapshot.mode: no_data`).

---

### PLAN DÉTAILLÉ DE LA PRÉSENTATION À GÉNÉRER :

#### PARTIE 1 : THÉORIE & FONDAMENTAUX (CDC & KAFKA)
- **Slide 1 : Titre & Introduction** (La problématique du découplage de données dans la migration moderne).
- **Slide 2 : L'Antipattern du "Dual-Write" vs le "Polling"** (Pourquoi demander à l'application d'écrire à 2 endroits échoue toujours, et pourquoi le polling périodique de base détruit les performances).
- **Slide 3 : Qu'est-ce que le Change Data Capture (CDC) ?** (Principe de capture des deltas au niveau du journal binaire/WAL, zéro impact applicatif, garantie de consistance).
- **Slide 4 : Apache Kafka comme Backbone d'Événements** (Pourquoi Kafka et pas une simple queue MQ ? Log immuable ordonné, persistance, découplage producteurs/consommateurs, scalabilité).
- **Slide 5 : L'Écosystème Kafka Connect & Debezium** (Architecture Source Connectors -> Topics -> Sink Connectors ; transformation d'un état relationnel en flux d'événements).

#### PARTIE 2 : ÉTUDE DE CAS & IMPLÉMENTATION SUR MON PROJET K8S
- **Slide 6 : Contexte du Projet & Objectif Métier** (Migration du domaine "Vétérinaires", isolation de `demo-db` vers `vet-db`, pattern Database-per-Service).
- **Slide 7 : Architecture Globale du Pipeline CDC Kubernetes** (Schéma complet : `demo-db` -> Debezium Source -> Kafka Topics -> JDBC Sink -> `vet-db`).
- **Slide 8 : La Configuration Source (PostgreSQL & WAL)** (Explication de `wal_level=logical`, `pgoutput`, et pourquoi le slot de réplication garantit la non-perte de transactions).
- **Slide 9 : Kafka KRaft & Kafka Connect sous Strimzi** (Déploiement déclaratif avec les Custom Resources Kubernetes : `KafkaNodePool`, `KafkaConnect`, gestion des secrets).
- **Slide 10 : Le Connecteur Debezium Source en Action** (Décorticage de la CRD `KafkaConnector` : tables surveillées, nommage des topics, configuration `snapshot.mode: no_data`).
- **Slide 11 : Le Connecteur JDBC Sink & L'Upsert** (Décorticage de la synchronisation vers `vet-db` : résolution des conflits avec `insert.mode: upsert`, propagation des clés primaires).
- **Slide 12 : Les Défis Rencontrés et Solutions Opérationnelles** (Gestion de la mémoire sous Kubernetes/Minikube, crashs JVM évités, séparation historique initial vs CDC continu).
- **Slide 13 : Conclusion & Métriques de Succès** (Zéro indisponibilité, cohérence éventuelle en quasi-temps réel, passerelle Nginx avec fallback).

Génère cette présentation en français, avec un ton professionnel, pédagogique et technique.
```

