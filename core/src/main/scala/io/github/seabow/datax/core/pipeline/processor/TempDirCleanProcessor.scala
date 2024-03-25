package io.github.seabow.datax.core.pipeline.processor

import io.github.seabow.datax.common.ConfigUtils._
import io.github.seabow.datax.common.{FutureUtils, HdfsUtils}
import io.github.seabow.datax.core.pipeline.Processor
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
object TempDirCleanProcessorConfig {
  val path_col = "path_col"
  //maybe somebody wants days or minutes ..but for now ,'hours' is enough
  val reserve_hours="reserve_hours"
  val max_concurrency = "max_concurrency"
}

class TempDirCleanProcessor extends Processor with Logging {
  def clearTemporaryDirectory(inputDF: DataFrame, path_col: String,reserveHours:Int,executorThreads:Int): DataFrame = {
    inputDF.foreachPartition {
      partition: Iterator[Row] =>
        val clearTasks: ListBuffer[Future[Any]] = ListBuffer.empty
        val ec = FutureUtils.buildExecutorContext(executorThreads)
        partition.foreach {
          r =>
            //FIXME: 当partitionOverwriteMode=dynamic时，temp目录格式为 .spark-staging-${job-id}/
            /**
             *see {@link org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol} for details
             */
            val tmpDir = r.getAs[String](path_col) + "/_temporary/0/_temporary/"
            if (HdfsUtils.exist(tmpDir)) {
              HdfsUtils.listDirs(tmpDir).foreach { status =>
                if (System.currentTimeMillis - status.getModificationTime > reserveHours * 60 * 60 * 1000) {
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
        println("Partition done.")
    }
    spark.emptyDataFrame
  }



  override def process(dfList: ListBuffer[DataFrame]): DataFrame = {
    val path_col = config.getString(TempDirCleanProcessorConfig.path_col, "path")
    val max_concurrency= config.getInt(TempDirCleanProcessorConfig.max_concurrency,300)
    val reserveHours= config.getInt(TempDirCleanProcessorConfig.reserve_hours,48)
    // this maybe a reasonable thread size as the request is sent to fs server,and the client is not too busy
    val default_executor_threads=20
    val max_partition_num=Math.ceil(max_concurrency.toDouble/default_executor_threads).toInt
    val inputDF = dfList(0)
    import io.github.seabow.datax.common.DataFrameUtils.DataFrameImplicits
    clearTemporaryDirectory(inputDF.repartitionByCount(default_executor_threads,max_partition_num),
      path_col,
      reserveHours,
      default_executor_threads)

  }

  override def shortName(): String = "temp_dir_clean"
}

