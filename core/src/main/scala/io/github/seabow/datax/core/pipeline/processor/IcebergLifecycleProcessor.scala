package io.github.seabow.datax.core.pipeline.processor

import io.github.seabow.datax.common.ConfigUtils._
import io.github.seabow.datax.common.{HdfsUtils, IcebergUtils}
import io.github.seabow.datax.core.pipeline.Processor
import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
object IcebergLifecycleProcessorConfig {

}

class IcebergLifecycleProcessor extends Processor with Logging {

  override def process(dfList: ListBuffer[DataFrame]): DataFrame = {
    // 顺序清理
    val inputDF=dfList.head
    inputDF.collect().foreach{
      row=>
        //步骤一:执行delete操作
        val tableName=row.getAs[String]("table_name")
        val retain_days= row.getAs[Int]("retain_days")
        val (partitionSpec,location)=IcebergUtils.getPartitionSpecAndLocation(tableName)
        val needToClearDir=HdfsUtils.listDirs(location).filter(
          System.currentTimeMillis()-_.getModificationTime>retain_days.day.toMillis).map(_.getPath.toString)
        val inPartitionValues= needToClearDir.map(_.split("=").last).map("'"+_+"'").mkString(",")
        val topPartitionCol=partitionSpec.split(",").head
        val clearSql=s"delete from $tableName where $topPartitionCol in ($inPartitionValues)"
        spark.sql(clearSql)
        //步骤二.执行expireSnapshots操作
          IcebergUtils.expireSnapshotsWithSpark(tableName)
        //步骤三.删除目录。
        //TODO 这里是否要加一个分布式删除？
        needToClearDir.foreach{
          dir=>
          HdfsUtils.delete(dir)
        }
    }
    spark.emptyDataFrame
  }

  override def shortName(): String = "iceberg_lifecycle"
}

