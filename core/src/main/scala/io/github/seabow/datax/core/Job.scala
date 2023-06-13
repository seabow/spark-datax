package io.github.seabow.datax.core

import io.github.seabow.datax.common._
import org.apache.spark.internal.Logging
import org.apache.spark.sql._

import java.util.UUID
import scala.collection.JavaConverters._
import scala.collection.mutable

case class Job(configContent: String, params: Map[String, String]=Map.empty, val spark: SparkSession)extends Logging{
  var outputMap: mutable.Map[String, DataFrame] = mutable.Map()
  val jobId = spark.sparkContext.applicationId + "_" + UUID.randomUUID()
  val jobDir = s"datax/job_dir/$jobId"
  val jobConfig = {
    val conf = ConfigUtils.parseAndResolveContent(configContent, params)
    println(conf)
    conf
  }

  def execute(): Unit = {
    val tasks = compile()
    if (!HdfsUtils.exist(jobDir)) {
      log.info(s"Make job dir $jobDir")
      HdfsUtils.mkdir(jobDir)
    }
    tasks.foreach(_.execute())
    if (HdfsUtils.exist(jobDir)) {
      if(HdfsUtils.delete(jobDir)){
        log.info(s"Deleted job dir $jobDir")
      }else {
        log.warn(s"Failed delete job dir $jobDir")
      }
    }
  }

  def compile(): Seq[Task] = {
    val taskConfigs = jobConfig.getConfigList("tasks").asScala
    val tasks = taskConfigs.map(Task(_, this))
    tasks
  }
}
