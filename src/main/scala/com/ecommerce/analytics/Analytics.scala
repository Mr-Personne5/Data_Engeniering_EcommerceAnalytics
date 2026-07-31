package com.ecommerce.analytics

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

class Analytics {

  // ---------------------------------------------------------------------
  // Question 4.1 - Rapport détaillé par marchand
  // ---------------------------------------------------------------------

  /** KPI par marchand : CA, transactions, clients uniques, panier moyen,
    * commission totale, classement par CA dans sa catégorie et dans sa région.
    * `enriched` doit provenir de DataTransformation.enrichTransactionData. */
  def merchantReport(enriched: DataFrame): DataFrame = {
    val perMerchant = enriched
      .groupBy("merchant_id", "merchant_name", "merchant_category", "region")
      .agg(
        sum("amount").as("chiffre_affaires_total"),
        count("*").as("nombre_transactions"),
        countDistinct("user_id").as("clients_uniques"),
        avg("amount").as("montant_moyen_transaction"),
        sum(col("amount") * col("commission_rate")).as("commission_totale")
      )

    val rankInCategory = Window.partitionBy("merchant_category").orderBy(col("chiffre_affaires_total").desc)
    val rankInRegion = Window.partitionBy("region").orderBy(col("chiffre_affaires_total").desc)

    perMerchant
      .withColumn("rang_ca_categorie", rank().over(rankInCategory))
      .withColumn("rang_ca_region", rank().over(rankInRegion))
  }

  /** Répartition du CA par marchand et par tranche d'âge client (une colonne
    * par tranche : "Jeune", "Adulte", "Age Moyen", "Senior"). */
  def salesByAgeBracket(enriched: DataFrame): DataFrame =
    enriched
      .groupBy("merchant_id", "merchant_name")
      .pivot("age_bracket", Seq("Jeune", "Adulte", "Age Moyen", "Senior"))
      .agg(round(sum("amount"), 2))
      .na.fill(0.0)

  // ---------------------------------------------------------------------
  // Question 4.2 - Analyse de cohortes utilisateurs
  // ---------------------------------------------------------------------

  /** Cohortes mensuelles : chaque utilisateur est rattaché au mois de sa 1ère
    * transaction (cohort_month). period_number = nombre de mois écoulés depuis
    * ce mois d'acquisition (0 = mois d'acquisition, 1 = mois suivant, ...).
    * Le taux de rétention compare, pour chaque période, le nombre de clients
    * encore actifs à la taille initiale de la cohorte. */
  def cohortAnalysis(enriched: DataFrame): DataFrame = {
    val firstTransactionWindow = Window.partitionBy("user_id")

    val withCohort = enriched
      .withColumn("cohort_month", date_trunc("month", min("event_time").over(firstTransactionWindow)))
      .withColumn("activity_month", date_trunc("month", col("event_time")))
      .withColumn("period_number", months_between(col("activity_month"), col("cohort_month")).cast("int"))

    val cohortSizes = withCohort
      .filter(col("period_number") === 0)
      .groupBy("cohort_month")
      .agg(countDistinct("user_id").as("taille_cohorte"))

    withCohort
      .groupBy("cohort_month", "period_number")
      .agg(
        countDistinct("user_id").as("clients_actifs"),
        sum("amount").as("chiffre_affaires")
      )
      .join(cohortSizes, "cohort_month")
      .withColumn("taux_retention_pct", round(col("clients_actifs") / col("taille_cohorte") * 100, 2))
      .orderBy("cohort_month", "period_number")
  }

  // ---------------------------------------------------------------------
  // Question 5.1 - Optimisation du stockage (cache d'un DataFrame réutilisé)
  // ---------------------------------------------------------------------

  /** `enriched` est lu 3 fois (merchantReport, salesByAgeBracket, cohortAnalysis)
    * -> mis en cache le temps des 3 calculs, puis libéré explicitement. */
  def generateFullReport(enriched: DataFrame): (DataFrame, DataFrame, DataFrame) = {
    val cached = enriched.cache()

    val merchants = merchantReport(cached)
    val ageBreakdown = salesByAgeBracket(cached)
    val cohorts = cohortAnalysis(cached)

    // Force le calcul du cache pendant qu'il est encore utile, avant de le libérer.
    merchants.count()
    ageBreakdown.count()
    cohorts.count()
    cached.unpersist()

    (merchants, ageBreakdown, cohorts)
  }
}
