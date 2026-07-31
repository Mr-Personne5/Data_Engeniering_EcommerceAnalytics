package com.ecommerce.analytics

import com.ecommerce.models.{Merchant, Product, Transaction, User}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.{AnalysisException, Dataset, SparkSession}
import org.apache.spark.storage.StorageLevel

/** Centralise la lecture, le typage et la validation des 4 sources de données. */
class DataIngestion(spark: SparkSession) {

  import spark.implicits._

  // ---------------------------------------------------------------------
  // Question 2.1 - Ingestion multi-format
  // ---------------------------------------------------------------------

  private val transactionsSchema = StructType(Seq(
    StructField("transaction_id", StringType, nullable = false),
    StructField("user_id", StringType, nullable = false),
    StructField("product_id", StringType, nullable = false),
    StructField("merchant_id", StringType, nullable = false),
    StructField("amount", DoubleType, nullable = false),
    StructField("timestamp", StringType, nullable = false),
    StructField("location", StringType, nullable = true),
    StructField("payment_method", StringType, nullable = true),
    StructField("category", StringType, nullable = true)
  ))

  private val usersSchema = StructType(Seq(
    StructField("user_id", StringType, nullable = false),
    StructField("age", IntegerType, nullable = false),
    StructField("annual_income", DoubleType, nullable = false),
    StructField("city", StringType, nullable = true),
    StructField("customer_segment", StringType, nullable = true),
    StructField("preferred_categories", ArrayType(StringType), nullable = true),
    StructField("registration_date", StringType, nullable = true)
  ))

  /** transactions.csv : schéma défini explicitement (timestamp doit rester une
    * chaîne, sinon l'inférence automatique le lirait comme un entier). */
  def readTransactions(path: String): Dataset[Transaction] =
    spark.read
      .schema(transactionsSchema)
      .option("header", "true")
      .csv(path)
      .as[Transaction]

  /** users.json : schéma explicite, avec preferred_categories déclaré en
    * ArrayType(StringType) pour bien mapper le champ imbriqué vers Seq[String]. */
  def readUsers(path: String): Dataset[User] =
    spark.read
      .schema(usersSchema)
      .json(path)
      .as[User]

  /** products.parquet : le schéma est déjà porté par le format Parquet. */
  def readProducts(path: String): Dataset[Product] =
    spark.read
      .parquet(path)
      .as[Product]

  /** merchants.csv : schéma laissé à l'inférence de Spark. establishment_date
    * (ex: "20220918") est alors inféré comme un entier -> on le recast en
    * String pour respecter le modèle de données. */
  def readMerchants(path: String): Dataset[Merchant] =
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path)
      .withColumn("establishment_date", col("establishment_date").cast(StringType))
      .as[Merchant]

  // ---------------------------------------------------------------------
  // Question 2.2 - Validation des données
  // ---------------------------------------------------------------------

  def validateTransactions(ds: Dataset[Transaction]): Dataset[Transaction] =
    ds.filter(t => t.amount > 0 && t.timestamp != null && t.timestamp.length == 14)

  def validateUsers(ds: Dataset[User]): Dataset[User] =
    ds.filter(u => u.age >= 16 && u.age <= 100 && u.annual_income > 0)

  def validateProducts(ds: Dataset[Product]): Dataset[Product] =
    ds.filter(p => p.price > 0 && p.rating >= 1 && p.rating <= 5)

  def validateMerchants(ds: Dataset[Merchant]): Dataset[Merchant] =
    ds.filter(m => m.commission_rate >= 0 && m.commission_rate <= 1)

  // ---------------------------------------------------------------------
  // Question 2.3 - Gestion d'erreurs et résumé
  // ---------------------------------------------------------------------

  /** Charge un dataset, le valide, affiche le bilan (lignes lues / valides) et
    * capture les erreurs de lecture (fichier introuvable, structure incorrecte...).
    *
    * `raw` est lu deux fois dans cette méthode (count avant validation, puis la
    * validation elle-même) -> on le stocke en mémoire (Question 5.1) le temps du
    * calcul, puis on le libère explicitement une fois `valid` obtenu :
    *  - `cache()` (= MEMORY_ONLY) pour les datasets de taille raisonnable.
    *  - `persist(MEMORY_AND_DISK_SER)` pour transactions.csv, le plus volumineux
    *    des 4 fichiers, afin d'éviter une perte de cache s'il ne tient pas
    *    entièrement en mémoire (et de réduire l'empreinte mémoire via la
    *    sérialisation). */
  private def loadWithSummary[T](
      datasetName: String,
      read: => Dataset[T],
      validate: Dataset[T] => Dataset[T],
      storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY
  ): Option[Dataset[T]] =
    try {
      val raw = read.persist(storageLevel)
      val rawCount = raw.count()
      println(s"[$datasetName] lignes lues avant validation : $rawCount")

      val valid = validate(raw)
      val validCount = valid.count()
      println(s"[$datasetName] lignes valides après validation : $validCount")

      raw.unpersist()
      Some(valid)
    } catch {
      case e: AnalysisException =>
        println(s"[$datasetName] Erreur de lecture (fichier introuvable ou structure incorrecte) : ${e.getMessage}")
        None
      case e: Exception =>
        println(s"[$datasetName] Erreur inattendue lors du chargement : ${e.getMessage}")
        None
    }

  def loadTransactions(path: String): Option[Dataset[Transaction]] =
    loadWithSummary(
      "transactions",
      readTransactions(path),
      validateTransactions,
      StorageLevel.MEMORY_AND_DISK_SER
    )

  def loadUsers(path: String): Option[Dataset[User]] =
    loadWithSummary("users", readUsers(path), validateUsers)

  def loadProducts(path: String): Option[Dataset[Product]] =
    loadWithSummary("products", readProducts(path), validateProducts)

  def loadMerchants(path: String): Option[Dataset[Merchant]] =
    loadWithSummary("merchants", readMerchants(path), validateMerchants)
}
