package io.github.seabow.datax.core

import io.github.seabow.datax.common._
import org.apache.spark.sql._

import java.util.UUID
import scala.collection.JavaConverters._
import scala.collection.mutable

case class Job(configContent: String, params: Map[String, String]=Map.empty, val spark: SparkSession) {
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
      HdfsUtils.mkdir(jobDir)
    }
    tasks.foreach(_.execute())
    if (HdfsUtils.exist(jobDir)) {
      HdfsUtils.delete(jobDir)
    }
  }

  def compile(): Seq[Task] = {
    val taskConfigs = jobConfig.getConfigList("tasks").asScala
    val tasks = taskConfigs.map(Task(_, this))
    tasks
  }
}
