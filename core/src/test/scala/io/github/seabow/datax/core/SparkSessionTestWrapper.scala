package io.github.seabow.datax.core

import org.apache.spark.sql.SparkSession

trait SparkSessionTestWrapper {

  val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("spark session")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.testing.memory", "2718592000")
      .config("spark.local.dir", "target/tmp")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.hive.exec.dynamic.partition.mode", "nonstrict")
      .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
      .config("spark.sql.catalog.spark_catalog.type", "hive")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.hive.metastore.schema.verification", "false")
      .enableHiveSupport()
      .getOrCreate()
  }

  def getConfContentFromPath(confPath: String): String = {
    val confContnet = scala.io.Source.fromFile(confPath, "utf8")
    confContnet.mkString
  }
}
