package com.ecommerce.analytics

import com.ecommerce.models.{Merchant, Product, TimeFeatures, Transaction, User}
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset}

import java.time.format.{DateTimeFormatter, TextStyle}
import java.time.{DayOfWeek, LocalDateTime}
import java.util.Locale

class DataTransformation {

  private val timestampPattern = "yyyyMMddHHmmss"

  // ---------------------------------------------------------------------
  // Question 3.1 - UDF extractTimeFeatures
  // ---------------------------------------------------------------------

  // Corps de l'UDF volontairement autonome (aucune référence à `this` ou à un
  // champ de la classe) : une closure qui capture l'instance DataTransformation
  // (non sérialisable) ferait échouer toute exécution distribuée avec
  // "Task not serializable".
  val extractTimeFeaturesUdf: UserDefinedFunction = udf { (timestamp: String) =>
    val formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss")
    val dt = LocalDateTime.parse(timestamp, formatter)
    val hour = dt.getHour
    val dayOfWeek = dt.getDayOfWeek

    val isWeekend = if (dayOfWeek == DayOfWeek.SATURDAY || dayOfWeek == DayOfWeek.SUNDAY) 1 else 0

    // Bornes demi-ouvertes, cohérentes avec l'énoncé ([6h-12[, [12h-18h[, [18h-22h[, [22h,+[).
    val dayPeriod =
      if (hour >= 6 && hour < 12) "Morning"
      else if (hour >= 12 && hour < 18) "Afternoon"
      else if (hour >= 18 && hour < 22) "Evening"
      else "Night"

    val isWorkingHours = if (hour >= 9 && hour < 17) 1 else 0

    TimeFeatures(
      hour = f"$hour%02d",
      day_of_week = dayOfWeek.getDisplayName(TextStyle.FULL, Locale.ENGLISH),
      month = dt.getMonth.getDisplayName(TextStyle.FULL, Locale.ENGLISH),
      is_weekend = isWeekend,
      day_period = dayPeriod,
      is_working_hours = isWorkingHours
    )
  }

  // ---------------------------------------------------------------------
  // Question 3.2 - enrichTransactionData
  // ---------------------------------------------------------------------

  /** Joint transactions/users/products/merchants, ajoute les caractéristiques
    * temporelles (UDF) et les indicateurs par utilisateur (Window functions). */
  def enrichTransactionData(
      transactions: Dataset[Transaction],
      users: Dataset[User],
      products: Dataset[Product],
      merchants: Dataset[Merchant]
  ): DataFrame = {

    // products et merchants partagent des noms de colonnes avec transactions
    // (merchant_id, category, name) -> renommage pour lever toute ambiguïté.
    val productsAliased = products
      .withColumnRenamed("name", "product_name")
      .withColumnRenamed("category", "product_category")
      .drop("merchant_id")

    val merchantsAliased = merchants
      .withColumnRenamed("name", "merchant_name")
      .withColumnRenamed("category", "merchant_category")

    // Question 5.2 : products (5 000 lignes) et merchants (500 lignes) sont
    // négligeables face aux 100 000 lignes de transactions -> broadcast() évite
    // un shuffle réseau coûteux sur la table volumineuse lors de la jointure.
    val joined = transactions
      .join(broadcast(productsAliased), Seq("product_id"), "left")
      .join(users, Seq("user_id"), "left")
      .join(broadcast(merchantsAliased), Seq("merchant_id"), "left")

    val userOrderWindow = Window.partitionBy("user_id").orderBy("event_time")
    val userCountWindow = Window.partitionBy("user_id")

    joined
      .withColumn("event_time", to_timestamp(col("timestamp"), timestampPattern))
      .withColumn("time_features", extractTimeFeaturesUdf(col("timestamp")))
      .select(col("*"), col("time_features.*"))
      .drop("time_features")
      .withColumn("transaction_rank_per_user", row_number().over(userOrderWindow))
      .withColumn("total_transactions_per_user", count("*").over(userCountWindow))
      .withColumn(
        "age_bracket",
        when(col("age") <= 25, "Jeune")
          .when(col("age") <= 44, "Adulte")
          .when(col("age") <= 64, "Age Moyen")
          .otherwise("Senior")
      )
  }

  // ---------------------------------------------------------------------
  // Question 3.3 - Analyse par partition Window
  // ---------------------------------------------------------------------

  private val sevenDaysInSeconds = 7L * 24 * 3600

  /** À appliquer sur le DataFrame retourné par enrichTransactionData (colonnes
    * user_id, event_time, amount requises). */
  def addRollingWindowFeatures(df: DataFrame): DataFrame = {
    val sevenDayWindow = Window
      .partitionBy("user_id")
      .orderBy(col("event_time").cast("long"))
      .rangeBetween(-sevenDaysInSeconds, 0)

    df
      .withColumn("amount_cumule_7j", sum("amount").over(sevenDayWindow))
      .withColumn("jours_actifs_7j", size(collect_set(to_date(col("event_time"))).over(sevenDayWindow)))
      .withColumn("utilisateur_actif", when(col("jours_actifs_7j") >= 5, 1).otherwise(0))
  }
}
