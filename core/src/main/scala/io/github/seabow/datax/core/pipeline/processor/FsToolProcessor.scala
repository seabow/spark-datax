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
import scala.concurrent.{Await, Future}

object FsToolProcessorConfig {
  val path_col = "path_col"
  val op = "op"
}

class FsToolProcessor extends Processor with Logging {
  override def process(dfList: ListBuffer[DataFrame]): DataFrame = {
    val path_col=config.getString(FsToolProcessorConfig.path_col,"path")
    val inputDF = dfList(0)
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
        Await.result(Future.sequence(opResultFutures), scala.concurrent.duration.Duration.Inf).filter(
          _.isDefined
        ).map(_.get)
    }(RowEncoder(newSchema))
  }

  override def shortName(): String = "assert"
}

