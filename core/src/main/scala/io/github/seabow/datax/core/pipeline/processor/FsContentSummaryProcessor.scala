package io.github.seabow.datax.core.pipeline.processor

import io.github.seabow.datax.common.ConfigUtils._
import io.github.seabow.datax.common.{FutureUtils, HdfsUtils}
import io.github.seabow.datax.core.pipeline.Processor
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import io.github.seabow.datax.common.DataFrameUtils.DataFrameImplicits
object FsContentSummaryProcessorConfig {
  val path_col = "path_col"
  val max_concurrency = "max_concurrency"
}

class FsContentSummaryProcessor extends Processor with Logging {
  def getContentSummary(inputDF: DataFrame, path_col: String,executor_threads:Int): DataFrame = {
    val newSchema = inputDF.schema.add("content_summary_length", LongType
    ).add("content_summary_dir_cnt", LongType
    ).add("content_summary_file_cnt", LongType
    ).add("content_summary_space_consumed", LongType)
    inputDF.mapPartitions {
      partition =>
       val executorContext=  FutureUtils.buildExecutorContext(executor_threads)
        val opResultFutures = partition.map {
          row =>
            val path = row.getAs[String](path_col)
            println(s"getting content summary for: $path ")
            try{
            FutureUtils.runWithTimeout(7200){
              val contentSummary = HdfsUtils.getContentSummaryWithThreads(20)(path)
              println(s"got content summary for: $path ,$contentSummary ")
              Row.fromSeq(row.toSeq ++ Seq[Any](contentSummary.getLength
                  , contentSummary.getDirectoryCount, contentSummary.getFileCount, contentSummary.getSpaceConsumed))
            }(executorContext)
              }catch {
              case e: Exception =>
                e.printStackTrace
                None
            }
        }
          val result=opResultFutures.toSeq.filter(_.isDefined).map(_.get).toIterator
        result
    }(RowEncoder(newSchema))
  }

  override def process(dfList: ListBuffer[DataFrame]): DataFrame = {
    val path_col = config.getString(FsContentSummaryProcessorConfig.path_col, "path")
    val max_concurrency= config.getInt(FsContentSummaryProcessorConfig.max_concurrency,300)
    // this maybe a reasonable thread size as the request is sent to fs server,and the client is not too busy
    val default_executor_threads=20
    val max_partition_num=Math.ceil(max_concurrency.toDouble/default_executor_threads).toInt
    val inputDF = dfList(0)
    getContentSummary(inputDF.repartitionByCount(default_executor_threads,max_partition_num), path_col,default_executor_threads)
  }

  override def shortName(): String = "fs_content_summary"
}

