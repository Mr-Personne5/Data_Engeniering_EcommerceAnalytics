name := "EcommerceAnalytics"
organization := "com.ecommerce"
version := "1.0.0"
scalaVersion := "2.12.18"

val sparkVersion = "3.5.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-sql"  % sparkVersion % Provided,
  "com.typesafe"      % "config"     % "1.4.3",
  "org.scalatest"     %% "scalatest" % "3.2.18" % Test
)

// Spark ne tourne pas encore sur Scala 2.13 en mode "full" -> on reste en 2.12.
// Les artefacts Spark sont en scope "Provided" : ils sont fournis par le cluster
// au moment du spark-submit et ne doivent pas être embarqués dans le fat-jar.

Compile / run / mainClass := Some("com.ecommerce.analytics.MainApp")
assembly / mainClass := Some("com.ecommerce.analytics.MainApp")
assembly / assemblyJarName := s"${name.value}-${version.value}.jar"

ThisBuild / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "reference.conf"              => MergeStrategy.concat
  case _                             => MergeStrategy.first
}

// "Provided" n'est pas sur le classpath de `sbt run` par défaut : on le rajoute
// pour pouvoir lancer l'app directement avec `sbt run` en local.
Compile / run := Defaults.runTask(Compile / fullClasspath, Compile / run / mainClass, Compile / run / runner).evaluated

// Spark 3.x accède à des classes internes du JDK (sun.nio.ch, java.nio, ...) via
// réflexion. Le module system de Java 17+ bloque ça par défaut -> il faut forker
// une JVM dédiée avec ces modules explicitement ouverts, pour `run` et `test`.
val javaOpensOptions = Seq(
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
  "--add-opens=java.base/java.io=ALL-UNNAMED",
  "--add-opens=java.base/java.net=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
  "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
  "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
)

Compile / run / fork := true
Compile / run / javaOptions ++= javaOpensOptions

Test / fork := true
Test / javaOptions ++= javaOpensOptions

// winutils.exe/hadoop.dll (Hadoop 3.3.x) : nécessaires uniquement sous Windows
// pour que Hadoop puisse lister un dossier sur le filesystem local (ex: lire
// products.parquet, qui est un dossier de fichiers de partitions). Scopé au
// projet : ni HADOOP_HOME ni PATH ne sont modifiés globalement sur la machine.
// (chemins relatifs à la base du projet, cwd de sbt lors du chargement du build)
val hadoopWinHome = new File("tools/hadoop-win").getAbsolutePath
val hadoopWinBin = new File("tools/hadoop-win/bin").getAbsolutePath
val hadoopWinEnv = Map(
  "HADOOP_HOME" -> hadoopWinHome,
  "PATH"        -> s"$hadoopWinBin;${sys.env.getOrElse("PATH", "")}"
)

Compile / run / envVars := hadoopWinEnv
Test / envVars := hadoopWinEnv
