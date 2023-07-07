package io.github.seabow.datax.common

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkFiles
import org.apache.spark.sql.SparkSession

import scala.io.Source

object SparkUtils {
  lazy val sparkSession = SparkSession.getActiveSession

  /**
   * while "--files" option of spark-submit won't upload files to driver in client mode,
   * this method make a way to fetch --file file content from executors to driver.
   *
   * @param fileName
   * @return
   */
  def getFileContent(fileName: String): String = {
    sparkSession.get.sparkContext.deployMode match {
      case "client" => Source.fromFile(fileName, "utf8").mkString
      case _ => sparkSession.get.sparkContext.parallelize(Seq(1)).repartition(1).map(r => Source.fromFile(SparkFiles.get(fileName)).mkString).first()
    }
  }

  def getHadoopConf(): Configuration = {
    sparkSession match {
      case Some(spark) => spark.sparkContext.hadoopConfiguration
      case None => new Configuration()
    }
  }
}
