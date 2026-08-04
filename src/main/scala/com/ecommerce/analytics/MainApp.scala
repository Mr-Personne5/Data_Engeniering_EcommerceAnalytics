package com.ecommerce.analytics

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.functions.{col, concat_ws}
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.{DataFrame, SparkSession}

/** Orchestration du pipeline complet : ingestion -> transformation -> analytique. */
object MainApp {

  def main(args: Array[String]): Unit = {
    val config = ConfigFactory.load()

    val spark = SparkSession.builder()
      .appName(config.getString("app.name"))
      .master(config.getString("app.spark.master"))
      .config("spark.sql.shuffle.partitions", config.getInt("app.spark.shuffle-partitions"))
      .getOrCreate()

    spark.sparkContext.setLogLevel(config.getString("app.spark.log-level"))

    // Question 6.1 - gestion des erreurs globale : quoi qu'il arrive pendant le
    // pipeline, la SparkSession est arrêtée proprement et l'erreur est reportée
    // sans laisser un processus JVM zombie.
    try {
      runPipeline(spark, config)
      println("\nPipeline terminé avec succès.")
    } catch {
      case e: Exception =>
        println(s"\n[MainApp] Erreur fatale lors de l'exécution du pipeline : ${e.getMessage}")
        e.printStackTrace()
        sys.exit(1)
    } finally {
      spark.stop()
    }
  }

  private def runPipeline(spark: SparkSession, config: Config): Unit = {
    val transactionsPath = config.getString("app.data.input.transactions")
    val usersPath = config.getString("app.data.input.users")
    val productsPath = config.getString("app.data.input.products")
    val merchantsPath = config.getString("app.data.input.merchants")
    val outputPath = config.getString("app.data.output.path")

    println("=== Phase 1/3 : Ingestion ===")
    val ingestion = new DataIngestion(spark)
    val transactionsOpt = ingestion.loadTransactions(transactionsPath)
    val usersOpt = ingestion.loadUsers(usersPath)
    val productsOpt = ingestion.loadProducts(productsPath)
    val merchantsOpt = ingestion.loadMerchants(merchantsPath)

    val loaded = for {
      transactions <- transactionsOpt
      users <- usersOpt
      products <- productsOpt
      merchants <- merchantsOpt
    } yield (transactions, users, products, merchants)

    loaded match {
      case None =>
        println("[MainApp] Pipeline interrompu : une ou plusieurs sources n'ont pas pu être chargées (voir erreurs ci-dessus).")

      case Some((transactions, users, products, merchants)) =>
        println("\n=== Phase 2/3 : Transformation ===")
        val transformation = new DataTransformation
        val enriched = transformation.enrichTransactionData(transactions, users, products, merchants)
        val enrichedWithWindows = transformation.addRollingWindowFeatures(enriched)

        println("\n=== Phase 3/3 : Analytique ===")
        val analytics = new Analytics
        val (merchantReport, salesByAge, cohorts) = analytics.generateFullReport(enrichedWithWindows)

        val consoleRows = config.getInt("app.display.console-rows")
        displayResults(enrichedWithWindows, merchantReport, salesByAge, cohorts, consoleRows)
        saveResults(enrichedWithWindows, merchantReport, salesByAge, cohorts, outputPath)
    }
  }

  private def displayResults(
      transactions: DataFrame,
      merchantReport: DataFrame,
      salesByAge: DataFrame,
      cohorts: DataFrame,
      consoleRows: Int
  ): Unit = {
    println("\n--- Transactions enrichies (extrait) ---")
    transactions.show(consoleRows, truncate = false)

    println("\n--- Rapport par marchand ---")
    merchantReport.orderBy(col("chiffre_affaires_total").desc).show(consoleRows, truncate = false)

    println("\n--- Répartition des ventes par tranche d'âge ---")
    salesByAge.show(consoleRows, truncate = false)

    println("\n--- Analyse de cohortes (rétention mensuelle) ---")
    cohorts.show(consoleRows, truncate = false)
  }

  private def saveResults(
      transactions: DataFrame,
      merchantReport: DataFrame,
      salesByAge: DataFrame,
      cohorts: DataFrame,
      outputPath: String
  ): Unit = {
    println(s"\n=== Sauvegarde des résultats dans $outputPath (CSV + Parquet) ===")

    val datasets = Seq(
      "transactions_enrichies" -> transactions,
      "rapport_marchands" -> merchantReport,
      "ventes_par_tranche_age" -> salesByAge,
      "cohortes" -> cohorts
    )

    datasets.foreach { case (name, df) =>
      // Le format CSV ne supporte pas les colonnes de type tableau (ex:
      // preferred_categories hérité de users) -> converties en texte pour le
      // CSV uniquement ; le Parquet, lui, gère nativement les types imbriqués.
      toCsvSafe(df).write.mode("overwrite").option("header", "true").csv(s"$outputPath/csv/$name")
      df.write.mode("overwrite").parquet(s"$outputPath/parquet/$name")
    }
  }

  private def toCsvSafe(df: DataFrame): DataFrame = {
    val arrayColumns = df.schema.fields.collect {
      case field if field.dataType.isInstanceOf[ArrayType] => field.name
    }
    arrayColumns.foldLeft(df) { (acc, columnName) =>
      acc.withColumn(columnName, concat_ws(", ", col(columnName)))
    }
  }
}
