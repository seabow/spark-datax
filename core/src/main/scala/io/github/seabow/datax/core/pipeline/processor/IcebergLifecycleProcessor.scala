package io.github.seabow.datax.core.pipeline.processor

import io.github.seabow.datax.common.ConfigUtils._
import io.github.seabow.datax.common.{HdfsUtils, IcebergUtils}
import io.github.seabow.datax.core.pipeline.Processor
import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame

import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._

object IcebergLifecycleProcessorConfig {
  val concurrent_clear_tables = "concurrent_clear_tables"
  val catalog_name = "catalog_name"
  val catalog_type = "catalog_type"
}

class IcebergLifecycleProcessor extends Processor with Logging {

  def clearTable(tableName: String, retainDays: Int, catalogType: String): Unit = {
    val (partitionSpec, location) = IcebergUtils.getPartitionSpecAndLocation(tableName)
    val iceberegTableDataDir=location+"/data"
    if(!HdfsUtils.exist(iceberegTableDataDir)){
      return
    }
    val needToClearDir = HdfsUtils.listDirs(iceberegTableDataDir).filter(
      System.currentTimeMillis() - _.getModificationTime > retainDays.day.toMillis).map(_.getPath.toString)
    log.warn(s"$tableName need to clear ${needToClearDir.size} directories.")
    if (needToClearDir.nonEmpty) {
      val inPartitionValues = needToClearDir.map(_.split("=").last).map("'" + _ + "'").mkString(",")
      val topPartitionCol = partitionSpec.split(",").head
      val clearSql = s"delete from $tableName where $topPartitionCol in ($inPartitionValues)"
      spark.sparkContext.setJobDescription(clearSql)
      spark.sql(clearSql)
      //步骤二.执行expireSnapshots操作
      spark.sparkContext.setJobDescription(s"expireSnapshotsWithSpark : tableName $tableName, catalogType $catalogType")
      IcebergUtils.expireSnapshotsWithSpark(tableName, catalogType)
      //步骤三.删除目录。
      spark.sparkContext.setJobDescription(s"clear ${needToClearDir.size} dirs:${needToClearDir.mkString(",")}")
      spark.sparkContext.parallelize(needToClearDir).foreachPartition {
        dirs:Iterator[String] =>
          val executor = Executors.newFixedThreadPool(20) // 这里设置了最大线程数为20
          val ec = ExecutionContext.fromExecutorService(executor)
          val clearTasks=ListBuffer.empty[Future[Any]]
          dirs.foreach{
            dir=>
              val clearTask=Future{HdfsUtils.delete(dir)}(ec)
            clearTasks.append(clearTask)
          }
          Await.result(Future.sequence(clearTasks),Duration.Inf)
          println("Partition Done!")
      }
    }
  }

  override def process(dfList: ListBuffer[DataFrame]): DataFrame = {
    val catalog_name = config.getString(IcebergLifecycleProcessorConfig.catalog_name, "iceberg")
    val catalog_type = config.getString(IcebergLifecycleProcessorConfig.catalog_type, "hive")
    val concurrent_clear_tables = config.getInt(IcebergLifecycleProcessorConfig.concurrent_clear_tables, 5)
    val inputDF = dfList.head
    val executor = Executors.newFixedThreadPool(concurrent_clear_tables)
    val ec = ExecutionContext.fromExecutorService(executor)
    val clearTasks = ListBuffer.empty[Future[Any]]
    val tablesClearInfos = inputDF.collect()
    val totalSize = tablesClearInfos.size
    val currentClearIndex = new AtomicInteger(1)
    tablesClearInfos.foreach {
      row =>
        //步骤一:执行delete操作
        var tableName = row.getAs[String]("table_name")
        if (tableName.split("\\.").size == 2) {
          tableName = catalog_name + "." + tableName
        }
        val retain_days = row.getAs[Int]("retain_days")
        val clearTask = Future {
          val jobGroup=s"$tableName (${currentClearIndex.getAndIncrement()}/$totalSize)"
          spark.sparkContext.setJobGroup(jobGroup, s"start clear $jobGroup")
          clearTable(tableName, retain_days, catalog_type)
        }(ec)
        clearTasks.append(clearTask)
    }
    Await.result(Future.sequence(clearTasks), Duration.Inf)
    spark.emptyDataFrame
  }

  override def shortName(): String = "iceberg_lifecycle"
}

