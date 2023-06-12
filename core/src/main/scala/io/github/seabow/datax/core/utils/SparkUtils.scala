package io.github.seabow.datax.core.utils

import org.apache.spark.SparkFiles
import org.apache.spark.sql.SparkSession

import scala.io.Source

object SparkUtils {
  /**
   * while "--files" option of spark-submit won't upload files to driver in client mode,
   * this method make a way to fetch --file file content from executors to driver.
   *
   * @param fileName
   * @return
   */
  def getFileContent(fileName: String, spark: SparkSession): String = {
    spark.sparkContext.parallelize(Seq(1)).repartition(1).map(r => Source.fromFile(SparkFiles.get(fileName)).mkString).first()
  }
}
