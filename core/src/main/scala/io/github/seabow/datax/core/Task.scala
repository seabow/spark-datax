package io.github.seabow.datax.core

import com.typesafe.config.Config
import io.github.seabow.datax.common.ConfigUtils._
import io.github.seabow.datax.core.pipeline.{Connector, Processor}
import org.apache.spark.datax.utils.ClassLoaderUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row}

import java.util.concurrent.ForkJoinPool
import scala.collection.mutable.ListBuffer
import scala.collection.parallel.ForkJoinTaskSupport

object ReaderConfig {
  /**
   * {
   * is_in:{
   * input: input_task_name
   * col: col_name
   * par: 10 //default 1 for serial, unlimit-> -1 represents the num of batch
   * batch_size:10000 //default input.size
   * max_batch: 10  // if both batch_size and max_batch is set,batch_size is max(batch_size,input.size/max_batch)
   * }
   * }
   */
  val is_in = "is_in"
}

object CommonConfig {
  //select_exprs
  val select_exprs = "select_exprs"

  //with_cols
  val with_cols = "with_cols"

  //repartition:
  val repartition = "repartition"
  //colsec
  val coalesce = "coalesce"

  val repartition_by_count = "repartition_by_count"

  //cache
  val filter = "filter"
  // cols rename
  val cols_rename = "cols_rename"

  // drop_cols
  val drop_cols = "drop_cols"

  val local_sort = "local_sort"

  val limit = "limit"
}

case class Task(config: Config, job: Job) extends Logging {
  def taskConfig: Config = config

  def execute(): Unit = {
    val shortName = config.getString("type")
    config.getString("stage") match {
      case "reader" =>
        val connector = ClassLoaderUtils.getPipelineInstance(shortName).asInstanceOf[Connector]
        connector.config(config).job(job)
        job.outputMap(config.getStringSafely("name")) = connector.read()
        dealWithReaderConfig()
        dealWithCommonConfig()
      case "writer" =>
        job.outputMap(config.getString("name")) = job.outputMap(config.getString("input"))
        dealWithCommonConfig()
        val connector = ClassLoaderUtils.getPipelineInstance(shortName).asInstanceOf[Connector]
        connector.config(config).job(job)
        connector.write(job.outputMap(config.getString("name")))
      case "processor" =>
        val input = config.getStringSafely("input")
        val dfList = new ListBuffer[DataFrame]
        if (input.nonEmpty) {
          val inputList = input.split(",")
          inputList.foreach(input => {
            require(job.outputMap.contains(input))
            dfList += job.outputMap(input)
          })
        }
        val processor = ClassLoaderUtils.getPipelineInstance(shortName).asInstanceOf[Processor]
        processor.config(config).job(job)
        job.outputMap(config.getString("name")) = processor.process(dfList)
        dealWithCommonConfig()
    }
  }

  def dealWithReaderConfig(): Unit = {
    var outputDF = job.outputMap(config.getString("name"))
    val is_in = config.getStringMapSafely("is_in")
    if (is_in.nonEmpty) {
      val isInInputDF = job.outputMap(is_in("input"))
      val par = is_in.getOrElse("par", "1").toInt
      val batch_size = is_in.getOrElse("batch_size", "-1").toInt
      val max_batch = is_in.getOrElse("max_batch", "-1").toInt
      val col_name = is_in.getOrElse("col", "")
      assert(isInInputDF != null && col_name.nonEmpty)
      val isInInputs = isInInputDF.collect().map(_.get(0))
      if (isInInputs.isEmpty) {
        println("is_in [], return an empty dataframe.")
        outputDF = job.spark.createDataFrame(job.spark.sparkContext.emptyRDD[Row], outputDF.schema)
      } else {
        var batch = 1
        if (batch_size > 0) {
          batch = math.ceil(isInInputs.length.toDouble / batch_size).toInt
          if (max_batch > 0) {
            batch = math.min(max_batch, batch)
          }
        }
        val actualBatchSize = Math.ceil(isInInputs.length.toDouble / batch).toInt
        val isInInputsBatches = isInInputs.grouped(actualBatchSize).toArray
        val parBatches = isInInputsBatches.par
        parBatches.tasksupport = new ForkJoinTaskSupport(new ForkJoinPool(par))
        outputDF = parBatches.map(
          batch =>
            outputDF.filter(col(col_name).isin(batch: _*))
        ).reduce((a, b) => a.union(b))
      }
    }
    job.outputMap(config.getString("name")) = outputDF
  }

  def dealWithCommonConfig(): Unit = {
    var outputDF = job.outputMap(config.getString("name"))
    //filter
    val filter = config.getString(CommonConfig.filter, "")
    if (!filter.isEmpty) {
      outputDF = outputDF.filter(filter)
    }

    val limit = config.getIntSafely(CommonConfig.limit)
    if (limit != 0) {
      outputDF = outputDF.limit(limit)
    }

    //select_exprs
    val selectExprs = config.getString(CommonConfig.select_exprs, "")
    if (!selectExprs.isEmpty) {
      outputDF = outputDF.selectExpr(selectExprs.split(","): _*)
    }

    //with_cols
    val withCols = config.getStringMapSafely(CommonConfig.with_cols)
    if (!withCols.isEmpty) {
      withCols.foreach {
        w =>
          outputDF = outputDF.withColumn(w._1, expr(w._2))
      }
    }

    //cols_rename
    val cols_rename = config.getStringMapSafely(CommonConfig.cols_rename)
    if (!cols_rename.isEmpty) {
      cols_rename.foreach {
        c =>
          outputDF = outputDF.withColumnRenamed(c._1, c._2)
      }
    }

    //drop_cols
    val drop_cols = config.getString(CommonConfig.drop_cols, "")
    if (!drop_cols.isEmpty) {
      outputDF = outputDF.drop(drop_cols.split(","): _*)
    }

    //repartition and coalesce
    val repartition = config.getInt(CommonConfig.repartition, -1)
    val coalesce = config.getInt(CommonConfig.coalesce, -1)
    val repartitionByCount = config.getStringSafely(CommonConfig.repartition_by_count)

    if (repartition > 0) {
      outputDF = outputDF.repartition(repartition)
    } else if (coalesce > 0) {
      outputDF = outputDF.coalesce(coalesce)
    } else if (repartitionByCount.nonEmpty) {
      val (countPerPartition, maxPartitionNum) = repartitionByCount.split(",") match {
        case Array(x, y) => (x.toLong, y.toInt)
        case Array(x) => (x.toLong, Int.MaxValue)
        case _ => throw new IllegalArgumentException("invalid repartition_by_count format. usage is: repartition_by_count: countPerPartition[,maxPartitionNum]")
      }
      val outputCount = outputDF.count
      val partitionNum = Math.min(outputCount / countPerPartition + 1, maxPartitionNum).toInt
      outputDF = outputDF.repartition(partitionNum)
      val taskName = config.getString("name")
      println(s"$taskName output count $outputCount ,repartition to $partitionNum.")
    }

    val local_sort = config.getStringSafely(CommonConfig.local_sort)
    if (local_sort.nonEmpty) {
      val localsortCols = local_sort.split(",").map(expr(_))
      outputDF = outputDF.sortWithinPartitions(localsortCols: _*)
    }

    job.outputMap(config.getString("name")) = outputDF
  }
}