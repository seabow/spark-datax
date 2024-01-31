package io.github.seabow.datax.core

import com.typesafe.config.ConfigRenderOptions
import io.github.seabow.datax.common._
import org.apache.spark.internal.Logging
import org.apache.spark.sql._

import java.util.UUID
import scala.collection.JavaConverters._
import scala.collection.mutable
import io.github.seabow.datax.common.ConfigUtils._
import org.apache.hadoop.fs.Path

case class Job(configContent: String, params: Map[String, String]=Map.empty, val spark: SparkSession)extends Logging{
  var outputMap: mutable.Map[String, DataFrame] = mutable.Map()
  val jobId = spark.sparkContext.applicationId + "_" + UUID.randomUUID()
  var jobDir = s"datax/job_dir/$jobId"
  var checkpointPath=s"datax/checkpoint/${spark.sparkContext.applicationId}"
  val jobConfig = {
    val conf = ConfigUtils.parseAndResolveContent(configContent, params)
    println(conf.root().render( ConfigRenderOptions.concise().setFormatted(true)))
    conf
  }
  var tasks=Seq.empty[Task]

  def setJobAttr():Unit={
    var base_dir=jobConfig.getString("base_dir","")
    if(base_dir.nonEmpty){
      base_dir=base_dir.stripSuffix("/")+"/"
      checkpointPath=s"$base_dir$checkpointPath"
      jobDir=s"$base_dir$jobDir"
    }
    if(spark.sparkContext.getCheckpointDir.isEmpty){
      spark.sparkContext.setCheckpointDir(checkpointPath)
      HdfsUtils.hdfs(new Path(checkpointPath) ).deleteOnExit(new Path(checkpointPath))
    }

  }

  def execute(): Unit = {
    tasks = compile()
    if (!HdfsUtils.exist(jobDir)) {
      log.info(s"Make job dir $jobDir")
      HdfsUtils.mkdir(jobDir)
    }
    HdfsUtils.hdfs(new Path(jobDir)).deleteOnExit(new Path(jobDir))
    tasks.foreach(_.execute())
  }

   def close(): Unit = {
     tasks.foreach(_.clear())
     if (HdfsUtils.exist(jobDir)) {
       if(HdfsUtils.delete(jobDir)){
         log.info(s"Deleted job dir $jobDir")
       }else {
         log.warn(s"Failed delete job dir $jobDir")
       }
     }
     if(HdfsUtils.exist(checkpointPath)){
       if(HdfsUtils.delete(checkpointPath)){
         log.info(s"Deleted checkpoint dir $checkpointPath")
       }else {
         log.warn(s"Failed delete checkpoint dir $checkpointPath")
       }
     }
  }


  def compile(): Seq[Task] = {
    setJobAttr()
    val taskConfigs = jobConfig.getConfigList("tasks").asScala
    val tasks = taskConfigs.map(Task(_, this))
    tasks
  }
}
