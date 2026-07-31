package com.ecommerce.analytics

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class AnalyticsSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private val dataDir = "src/main/resources/data"

  private var spark: SparkSession = _
  private var enriched: DataFrame = _
  private var analytics: Analytics = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("AnalyticsSpec")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val ingestion = new DataIngestion(spark)
    val transformation = new DataTransformation

    val transactions = ingestion.loadTransactions(s"$dataDir/transactions.csv").get
    val users = ingestion.loadUsers(s"$dataDir/users.json").get
    val products = ingestion.loadProducts(s"$dataDir/products.parquet").get
    val merchants = ingestion.loadMerchants(s"$dataDir/merchants.csv").get

    enriched = transformation.enrichTransactionData(transactions, users, products, merchants)
    analytics = new Analytics
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
  }

  "merchantReport" should "calculer les KPI par marchand avec des classements cohérents" in {
    val report = analytics.merchantReport(enriched)

    report.count() should be > 0L
    report.filter(col("chiffre_affaires_total") <= 0).count() shouldBe 0
    report.filter(col("rang_ca_categorie") < 1).count() shouldBe 0
    report.filter(col("rang_ca_region") < 1).count() shouldBe 0

    // Le total des CA par marchand doit être égal au CA global des transactions.
    val totalFromReport = report.agg(sum("chiffre_affaires_total")).head().getDouble(0)
    val totalFromTransactions = enriched.agg(sum("amount")).head().getDouble(0)
    totalFromReport shouldBe totalFromTransactions +- 0.01
  }

  "salesByAgeBracket" should "répartir le CA par tranche d'âge et rester cohérent avec le CA total du marchand" in {
    val byAge = analytics.salesByAgeBracket(enriched)
    val report = analytics.merchantReport(enriched)

    Seq("Jeune", "Adulte", "Age Moyen", "Senior").foreach(c => byAge.columns should contain(c))

    val joined = byAge.join(report.select("merchant_id", "chiffre_affaires_total"), "merchant_id")
    // somme des 4 tranches doit correspondre au CA total du marchand
    val diffs = joined
      .selectExpr("merchant_id", "chiffre_affaires_total", "(Jeune + Adulte + `Age Moyen` + Senior) as somme_tranches")
      .withColumn("diff", abs(col("chiffre_affaires_total") - col("somme_tranches")))
    diffs.filter(col("diff") > 0.05).count() shouldBe 0
  }

  "cohortAnalysis" should "produire une table de rétention cohérente (période 0 = 100%)" in {
    val cohorts = analytics.cohortAnalysis(enriched)

    cohorts.count() should be > 0L
    cohorts.filter(col("period_number") < 0).count() shouldBe 0
    cohorts.filter(col("taux_retention_pct") > 100.0).count() shouldBe 0

    val period0 = cohorts.filter(col("period_number") === 0)
    period0.filter(col("taux_retention_pct") =!= 100.0).count() shouldBe 0
  }

  "generateFullReport" should "mettre en cache enriched, produire les 3 rapports et libérer le cache" in {
    val (merchants, ageBreakdown, cohorts) = analytics.generateFullReport(enriched)

    merchants.count() should be > 0L
    ageBreakdown.count() should be > 0L
    cohorts.count() should be > 0L

    enriched.storageLevel.useMemory shouldBe false
  }
}
