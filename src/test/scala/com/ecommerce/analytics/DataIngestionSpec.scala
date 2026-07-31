package com.ecommerce.analytics

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DataIngestionSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private val dataDir = "src/main/resources/data"

  private var spark: SparkSession = _
  private var ingestion: DataIngestion = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("DataIngestionSpec")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    ingestion = new DataIngestion(spark)
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
  }

  "readTransactions" should "typer transactions.csv en Dataset[Transaction] avec le schéma explicite" in {
    val ds = ingestion.readTransactions(s"$dataDir/transactions.csv")
    ds.count() shouldBe 100000
    val first = ds.head()
    first.timestamp.length shouldBe 14
  }

  "readUsers" should "typer users.json en Dataset[User] avec preferred_categories en Seq[String]" in {
    val ds = ingestion.readUsers(s"$dataDir/users.json")
    ds.count() shouldBe 10000
    ds.head().preferred_categories should not be empty
  }

  "readProducts" should "typer products.parquet en Dataset[Product]" in {
    val ds = ingestion.readProducts(s"$dataDir/products.parquet")
    ds.count() should be > 0L
  }

  "readMerchants" should "typer merchants.csv en Dataset[Merchant] avec establishment_date en String" in {
    val ds = ingestion.readMerchants(s"$dataDir/merchants.csv")
    ds.count() shouldBe 500
    ds.head().establishment_date.length shouldBe 8
  }

  "loadTransactions" should "afficher le bilan lignes lues / valides et renvoyer les données valides" in {
    val result = ingestion.loadTransactions(s"$dataDir/transactions.csv")
    result shouldBe defined
    result.get.count() should be <= 100000L
  }

  it should "retourner None et ne pas lever d'exception si le fichier est introuvable" in {
    val result = ingestion.loadTransactions(s"$dataDir/does_not_exist.csv")
    result shouldBe None
  }

  "loadUsers" should "afficher le bilan lignes lues / valides" in {
    ingestion.loadUsers(s"$dataDir/users.json") shouldBe defined
  }

  "loadProducts" should "afficher le bilan lignes lues / valides" in {
    ingestion.loadProducts(s"$dataDir/products.parquet") shouldBe defined
  }

  "loadMerchants" should "afficher le bilan lignes lues / valides" in {
    ingestion.loadMerchants(s"$dataDir/merchants.csv") shouldBe defined
  }
}
