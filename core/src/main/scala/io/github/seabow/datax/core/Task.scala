package io.github.seabow.datax.core
import com.typesafe.config.Config
import io.github.seabow.datax.common.ConfigUtils._
import io.github.seabow.datax.core.pipeline.{Connector, Processor}
import org.apache.spark.datax.utils.ClassLoaderUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.expr

import scala.collection.mutable.ListBuffer

object CommonConfig{
  //select_exprs
  val select_exprs="select_exprs"

  //with_cols
  val with_cols="with_cols"

  //repartition:
  val repartition="repartition"
  //colsec
  val coalesce="coalesce"
  //cache
  val filter="filter"
  // cols rename
  val cols_rename="cols_rename"

  // drop_cols
  val drop_cols="drop_cols"

  val limit = "limit"
}
case class Task(config:Config,job: Job) extends Logging{
  def taskConfig: Config =config
  def execute():Unit={
    val shortName=config.getString("type")
    config.getString("stage")match {
      case "reader"=>
        val connector=ClassLoaderUtils.getPipelineInstance(shortName).asInstanceOf[Connector]
        connector.config(config).job(job)
        job.outputMap(config.getString("name"))=connector.read()
        dealWithCommonConfig()
      case "writer"=>
        job.outputMap(config.getString("name"))=job.outputMap(config.getString("input"))
        dealWithCommonConfig()
        val connector=ClassLoaderUtils.getPipelineInstance(shortName).asInstanceOf[Connector]
        connector.config(config).job(job)
        connector.write(job.outputMap(config.getString("name")))
      case "processor"=>
        val input = config.getString( "input")
        val dfList = new ListBuffer[DataFrame]
        if(input.nonEmpty){
          val inputList = input.split(",")
          inputList.foreach(input => {
            require(job.outputMap.contains(input))
            dfList += job.outputMap(input)
          })
        }
        val processor=ClassLoaderUtils.getPipelineInstance(shortName).asInstanceOf[Processor]
        processor.config(config).job(job)
        job.outputMap(config.getString("name"))=processor.process(dfList)
        dealWithCommonConfig()
    }
  }
  def dealWithCommonConfig():Unit={
    var outputDF=job.outputMap(config.getString("name"))
    //filter
    val filter=config.getString(CommonConfig.filter,"")
    if(!filter.isEmpty){
      outputDF=outputDF.filter(filter)
    }

    val limit=config.getIntSafely(CommonConfig.limit)
    if(limit !=0){
      outputDF=outputDF.limit(limit)
    }

    //select_exprs
    val selectExprs=config.getString(CommonConfig.select_exprs,"")
    if(!selectExprs.isEmpty){
      outputDF= outputDF.selectExpr(selectExprs.split(","):_*)
    }

    //with_cols
    val withCols=config.getStringMapSafely(CommonConfig.with_cols)
    if(!withCols.isEmpty){
      withCols.foreach{
        w=>
          outputDF=outputDF.withColumn(w._1,expr(w._2))
      }
    }

    //cols_rename
    val cols_rename=config.getStringMapSafely(CommonConfig.cols_rename)
    if(!cols_rename.isEmpty){
      cols_rename.foreach{
        c=>
          outputDF=outputDF.withColumnRenamed(c._1,c._2)
      }
    }

    //drop_cols
    val drop_cols=config.getString(CommonConfig.drop_cols,"")
    if(!drop_cols.isEmpty){
      outputDF=outputDF.drop(drop_cols.split(","):_*)
    }

    //repartition and coalesce
    val repartition=config.getInt(CommonConfig.repartition,-1)
    val coalesce=config.getInt(CommonConfig.coalesce,-1)

    if(repartition>0){
      outputDF=outputDF.repartition(repartition)
    }else if(coalesce>0){
      outputDF=outputDF.coalesce(coalesce)
    }
    job.outputMap(config.getString("name"))=outputDF
  }
}