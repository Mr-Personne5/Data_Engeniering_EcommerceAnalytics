# EcommerceAnalytics

Pipeline Spark/Scala d'analyse de données e-commerce : ingestion multi-format
(CSV, JSON, Parquet), validation, enrichissement temporel (UDF + fenêtres
glissantes), KPI marchands et analyse de cohortes de clients.

## Fonctionnalités

- **Ingestion typée** de 4 sources hétérogènes vers des `Dataset[T]` (schéma
  explicite, inféré ou porté par le format selon la source) avec validation
  des règles métier et bilan lignes lues / lignes valides.
- **Enrichissement des transactions** : caractéristiques temporelles (heure,
  jour de semaine, mois, week-end, période de la journée, heures ouvrées) via
  une UDF, jointure avec utilisateurs/produits/marchands, rang et nombre de
  transactions par utilisateur, tranche d'âge.
- **Comportement utilisateur** : montant cumulé et statut "actif" sur une
  fenêtre glissante de 7 jours (Spark Window functions).
- **Analytique business** : rapport de performance par marchand (CA,
  transactions, clients uniques, panier moyen, commission, classements par
  catégorie/région, répartition par tranche d'âge) et analyse de cohortes de
  rétention mensuelle.
- **Optimisations Spark** : `cache()` / `persist(MEMORY_AND_DISK_SER)` /
  `unpersist()` sur les DataFrames réutilisés, `broadcast()` sur les tables de
  dimension (`products`, `merchants`) lors des jointures avec `transactions`.

## Modèle de données

| Fichier | Format | Lignes | Contenu |
|---|---|---|---|
| `transactions.csv` | CSV, schéma explicite | 100 000 | Achats (montant, timestamp, moyen de paiement...) |
| `users.json` | JSON, schéma explicite | 10 000 | Clients (âge, revenu, catégories préférées...) |
| `products.parquet` | Parquet | 5 000 | Catalogue produit (prix, note, stock...) |
| `merchants.csv` | CSV, schéma inféré | 500 | Marchands (catégorie, région, commission...) |

Les case classes correspondantes (`Transaction`, `User`, `Product`,
`Merchant`, `TimeFeatures`) sont dans `com.ecommerce.models`.

## Prérequis

- **JDK 17** (Spark 3.5.x ne supporte pas Java 21+/25). Sur cette machine, un
  JDK 17 est disponible dans `%USERPROFILE%\.jdks\ms-17.0.18` :
  ```powershell
  $env:JAVA_HOME = "$env:USERPROFILE\.jdks\ms-17.0.18"
  ```
- **Scala 2.12.18** et **sbt 1.9.9** : gérés automatiquement par le lanceur sbt
  déjà installé (aucune installation manuelle requise ; sinon voir
  https://www.scala-sbt.org/download/).
- **Apache Spark 3.5.1** installé localement uniquement si vous comptez lancer
  `spark-submit` directement (le mode `sbt run`/`sbt test` n'en a pas besoin,
  Spark est fourni comme dépendance gérée par sbt).
- **Windows uniquement** : `winutils.exe`/`hadoop.dll` (Hadoop 3.3.x) sont
  nécessaires pour que Spark puisse lister un dossier sur le filesystem local
  (ex. lire `products.parquet`, qui est un dossier de fichiers de partitions).
  Ils sont déjà fournis dans `tools/hadoop-win/bin/` et configurés dans
  `build.sbt` (variables d'environnement scopées au projet, rien de modifié
  globalement sur la machine) — aucune action requise.

## Compilation et tests

Depuis le dossier `EcommerceAnalytics/` :

```powershell
sbt compile        # compile le projet
sbt test           # exécute les tests unitaires (sur les vraies données du projet)
sbt assembly       # génère le JAR exécutable (fat-jar) dans target/scala-2.12/
```

Le JAR généré s'appelle `EcommerceAnalytics-1.0.0.jar`.

## Exécution locale (avec sbt)

```powershell
sbt run
```

Utilise `local[*]` comme master Spark (configuré dans `application.conf`) : aucune
installation Spark séparée n'est nécessaire, sbt télécharge les dépendances.

## Déploiement (spark-submit sur un cluster)

Après `sbt assembly` :

```bash
spark-submit \
  --class com.ecommerce.analytics.MainApp \
  --master <url-du-cluster>  \
  --deploy-mode cluster \
  target/scala-2.12/EcommerceAnalytics-1.0.0.jar
```

(Remplacer `<url-du-cluster>` par ex. `yarn`, `spark://host:7077`, ou `local[*]`
pour tester en local avec le vrai binaire `spark-submit`.) Les chemins de
`application.conf` étant relatifs (`src/main/resources/data/...`), pensez à
les adapter ou à les surcharger (`-Dconfig.file=...`) si vous exécutez le JAR
depuis un autre répertoire ou sur un cluster distant.

## Structure du projet

```
EcommerceAnalytics/
├── build.sbt
├── project/
│   ├── build.properties
│   └── plugins.sbt
├── README.md
├── tools/hadoop-win/bin/      (winutils.exe, hadoop.dll — dev local Windows)
└── src/
    ├── main/
    │   ├── scala/com/ecommerce/
    │   │   ├── analytics/     (DataIngestion, DataTransformation, Analytics, MainApp)
    │   │   └── models/        (case classes du domaine)
    │   └── resources/
    │       ├── application.conf
    │       └── data/          (transactions.csv, users.json, products.parquet, merchants.csv)
    └── test/scala/com/ecommerce/analytics/
        (DataIngestionSpec, DataTransformationSpec, AnalyticsSpec)
```

## État d'avancement

- [x] Partie 1 — Structure du projet, `build.sbt`, `README.md`
- [x] Partie 2 — Ingestion multi-format et validation des données
- [x] Partie 3 — UDF `extractTimeFeatures`, `enrichTransactionData`, fenêtres glissantes
- [x] Partie 4 — Rapport par marchand et analyse de cohortes
- [x] Partie 5 — Optimisations Spark (cache/persist/unpersist, broadcast)
- [ ] Partie 6 — `MainApp` (orchestration complète, sauvegarde des résultats)
- [ ] Partie 7 — Configuration externalisée complète (`application.conf`)
