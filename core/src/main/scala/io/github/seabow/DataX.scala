package io.github.seabow

import io.github.seabow.datax.common.{ParameterUtils, SparkUtils}
import io.github.seabow.datax.core.Job
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

import scala.io.Source

object DataX extends Logging {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .set("spark.port.maxRetries", "30")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.sql.broadcastTimeout", "3000")
      .set("spark.driver.maxResultSize", "4g")
    //获取参数
    val params = ParameterUtils.getParameters(args)
    //默认位置和获取的参数。
    val configPath= params.get("config_path")
    if(!configPath.isDefined){
      throw new IllegalArgumentException("No config_path specified")
    }

    val spark: SparkSession = SparkSession
      .builder()
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()
    val appId = spark.sparkContext.applicationId
    val uiWebUrl = spark.sparkContext.uiWebUrl
    log.warn(s"application id : $appId")
    log.warn(s"app web url : $uiWebUrl")
    val configContent=spark.sparkContext.deployMode match {
      case "client"=>  Source.fromFile(configPath.get,"utf8").mkString
      case  _=>  SparkUtils.getFileContent(configPath.get)
    }
    val job=Job(configContent,params,spark)
    try{
      job.execute()
    }finally{
      job.close()
    }
  }
}
