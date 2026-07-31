package com.ecommerce.analytics

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DataTransformationSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private val dataDir = "src/main/resources/data"

  private var spark: SparkSession = _
  private var ingestion: DataIngestion = _
  private var transformation: DataTransformation = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("DataTransformationSpec")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    ingestion = new DataIngestion(spark)
    transformation = new DataTransformation
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
  }

  "extractTimeFeaturesUdf" should "dériver correctement heure/jour/mois/flags à partir d'un timestamp connu" in {
    val sparkSession = spark
    import sparkSession.implicits._
    val df = Seq("20240701021822").toDF("timestamp")
      .withColumn("tf", transformation.extractTimeFeaturesUdf(col("timestamp")))
      .select("tf.*")

    val row = df.head()
    row.getAs[String]("hour") shouldBe "02"
    row.getAs[String]("day_of_week") shouldBe "Monday"
    row.getAs[String]("month") shouldBe "July"
    row.getAs[Int]("is_weekend") shouldBe 0
    row.getAs[String]("day_period") shouldBe "Night"
    row.getAs[Int]("is_working_hours") shouldBe 0
  }

  "enrichTransactionData" should "joindre les 4 sources et ajouter les colonnes attendues sans ambiguïté" in {
    val transactions = ingestion.loadTransactions(s"$dataDir/transactions.csv").get
    val users = ingestion.loadUsers(s"$dataDir/users.json").get
    val products = ingestion.loadProducts(s"$dataDir/products.parquet").get
    val merchants = ingestion.loadMerchants(s"$dataDir/merchants.csv").get

    val enriched = transformation.enrichTransactionData(transactions, users, products, merchants)

    enriched.count() shouldBe transactions.count()

    val expectedCols = Seq(
      "hour", "day_of_week", "month", "is_weekend", "day_period", "is_working_hours",
      "transaction_rank_per_user", "total_transactions_per_user", "age_bracket",
      "product_name", "product_category", "merchant_name", "merchant_category"
    )
    expectedCols.foreach(c => enriched.columns should contain(c))

    val tx1 = enriched.filter(col("transaction_id") === "TX000001").head()
    tx1.getAs[String]("hour") shouldBe "02"
    tx1.getAs[String]("day_of_week") shouldBe "Monday"
    tx1.getAs[String]("day_period") shouldBe "Night"
    tx1.getAs[String]("age_bracket") should (be("Jeune") or be("Adulte") or be("Age Moyen") or be("Senior"))
  }

  "addRollingWindowFeatures" should "calculer un montant cumulé cohérent et un flag utilisateur actif binaire" in {
    val transactions = ingestion.loadTransactions(s"$dataDir/transactions.csv").get
    val users = ingestion.loadUsers(s"$dataDir/users.json").get
    val products = ingestion.loadProducts(s"$dataDir/products.parquet").get
    val merchants = ingestion.loadMerchants(s"$dataDir/merchants.csv").get

    val enriched = transformation.enrichTransactionData(transactions, users, products, merchants)
    val withWindows = transformation.addRollingWindowFeatures(enriched)

    withWindows.columns should contain("amount_cumule_7j")
    withWindows.columns should contain("utilisateur_actif")

    val distinctFlags = withWindows.select("utilisateur_actif").distinct().collect().map(_.getInt(0)).toSet
    distinctFlags.subsetOf(Set(0, 1)) shouldBe true

    // Pour la 1ère transaction (rang 1) d'un utilisateur, le montant cumulé sur 7 jours
    // doit être >= au montant de cette transaction (elle-même incluse dans la fenêtre).
    val firstTx = withWindows.filter(col("transaction_rank_per_user") === 1).head()
    firstTx.getAs[Double]("amount_cumule_7j") should be >= firstTx.getAs[Double]("amount")
  }
}
