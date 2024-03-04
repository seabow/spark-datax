package io.github.seabow.datax.core.pipeline.processor

import io.github.seabow.datax.common.ConfigUtils._
import io.github.seabow.datax.common.HdfsUtils
import io.github.seabow.datax.core.pipeline.Processor
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import java.util.concurrent.Executors
import scala.concurrent.ExecutionContext
object FsToolProcessorConfig {
  val path_col = "path_col"
  val op = "op"

}

class FsToolProcessor extends Processor with Logging {
  def getContentSummary(inputDF: DataFrame, path_col: String): DataFrame = {
    val newSchema = inputDF.schema.add("content_summary_length", LongType
    ).add("content_summary_dir_cnt", LongType
    ).add("content_summary_file_cnt", LongType
    ).add("content_summary_space_consumed", LongType)
    inputDF.mapPartitions {
      partition =>
        val opResultFutures = partition.map {
          row =>
            val path = row.getAs[String](path_col)
            Future {
              try {
                val contentSummary = HdfsUtils.getContentSummary(path)
                Some(Row.fromSeq(row.toSeq ++ Seq[Any](contentSummary.getLength
                  , contentSummary.getDirectoryCount, contentSummary.getFileCount, contentSummary.getSpaceConsumed)))
              } catch {
                case e: Exception => None
              }
            }
        }
        Await.result(Future.sequence(opResultFutures), Duration.Inf).filter(
          _.isDefined
        ).map(_.get)
    }(RowEncoder(newSchema))
  }

  def clearTemporaryDirectory(inputDF: DataFrame, path_col: String): DataFrame = {
    inputDF.select(path_col).foreachPartition {
      partition: Iterator[Row] =>
        val clearTasks: ListBuffer[Future[Any]] = ListBuffer.empty
        val executor = Executors.newFixedThreadPool(20) // 这里设置了最大线程数为10
        val ec = ExecutionContext.fromExecutorService(executor)
        partition.foreach {
          r =>
            val tmpDir = r.getString(0) + "/_temporary/0/_temporary/"
            if (HdfsUtils.exist(tmpDir)) {
              HdfsUtils.listDirs(tmpDir).foreach { status =>
                if (System.currentTimeMillis - status.getModificationTime > 48 * 60 * 60 * 1000) {
                  val clearTask = Future {
                    HdfsUtils.delete(status.getPath.toString);
                    println(s"Deleted ${status.getPath.toString}")
                  }(ec)
                  clearTasks.append(clearTask)
                }
              }
            }
        }
        Await.result(Future.sequence(clearTasks), Duration.Inf)
        println("Partition done!")
    }
    spark.emptyDataFrame
  }
  override def process(dfList: ListBuffer[DataFrame]): DataFrame = {
    val path_col = config.getString(FsToolProcessorConfig.path_col, "path")
    val op = config.getString(FsToolProcessorConfig.op, "get_content_summary")
    val inputDF = dfList(0)
    val result = op match {
      case "get_content_summary" => getContentSummary(inputDF, path_col)
      case "clear_temporary_directory" => clearTemporaryDirectory(inputDF, path_col)
    }
    result
  }

  override def shortName(): String = "fs_tool"
}

