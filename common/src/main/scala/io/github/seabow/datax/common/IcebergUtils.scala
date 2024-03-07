package io.github.seabow.datax.common

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.iceberg.CatalogUtil
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.spark.actions._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataType, StructType}

import java.util.concurrent.Executors
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

object IcebergUtils extends Logging{
  def getPartitionSpecAndLocation(table:String):(String,String)={
    val spark=SparkSession.getActiveSession.get
    val describeInfo=spark.sql(s"describe EXTENDED $table").collect()
    val location=describeInfo.filter(_.getAs[String]("col_name").equals("Location")).head.getAs[String]("data_type")
    val partitionSpecDDL=describeInfo.filter(_.getAs[String]("col_name").equals("_partition")).head.getAs[String]("data_type")
    val partitionSpec=DataType.fromDDL(partitionSpecDDL).asInstanceOf[StructType].fieldNames.mkString(",")
    log.warn(s"partitionSpec : [$partitionSpec] , Location : $location")
    (partitionSpec,location)
  }
   def expireSnapshotsWithSpark(tableName:String,catalogType:String): Unit ={
     val tableNamePart=tableName.split("\\.")
     val catalogName=tableNamePart.head
     val namespace=tableNamePart(1)
     val shortTableName=tableNamePart.last
     val sparkContext=SparkSession.getActiveSession.get.sparkContext
   val catalog=CatalogUtil.buildIcebergCatalog(catalogName,Map("type"->catalogType).asJava,sparkContext.hadoopConfiguration)
     catalog.initialize(catalogName,Map("type"->"hive").asJava)
    val table= catalog.loadTable(TableIdentifier.of(namespace, shortTableName))
    val action=SparkActions.get.expireSnapshots(table).expireOlderThan(System.currentTimeMillis()
    ).retainLast(1)
     //expire metadata
     var filesToClear=action.expireFiles().repartition(30).cache()
     var clearFileCnt=filesToClear.count()
     // clear file using spark
     sparkContext.setJobDescription(s"expire snapshots: deleting $clearFileCnt files.")
     filesToClear.foreachPartition{
       partition: Iterator[FileInfo]=>
         val clearTasks: ListBuffer[Future[Any]] = ListBuffer.empty
         val executor = Executors.newFixedThreadPool(20) // 这里设置了最大线程数为20
         val ec = ExecutionContext.fromExecutorService(executor)
         partition.foreach{
           fileInfo=>
            val clearTask=Future( try{
              FileSystem.get(new Path(fileInfo.getPath).toUri,new Configuration()).delete(new Path(fileInfo.getPath),false)
             }catch {
               case e:Throwable =>
             })(ec)
             clearTasks.append(clearTask)
         }
         Await.result(Future.sequence(clearTasks),Duration.Inf)
         println("Partition Done")
     }
     filesToClear.unpersist(true)
   }
}
