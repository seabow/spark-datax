package io.github.seabow.datax.core.pipeline.connector

import io.github.seabow.datax.common.ConfigUtils.ImplicitConfigUtils
import io.github.seabow.datax.common.{HiveUtils, IcebergUtils}
import io.github.seabow.datax.core.pipeline.Connector
import org.apache.spark.Success
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.utils.SparkCatalogUtils

import scala.collection.mutable

object HiveConnectorConfig{
  def table = "table"
  def partition_by = "partition_by"
  //partition_spec as: partition_col_1='partition_value_1',partition_col_2='partition_value_2'
  def partition_spec = "partition_spec"
  def mode = "mode"
  def options = "options"
}

class HiveConnector extends Connector with Logging{
  override def shortName(): String = "hive"
  override def read(): DataFrame = {
    val table: String = config.getString( HiveConnectorConfig.table)
    /*读取数据源表*/
   spark.read.table(table)
  }

  def reorderDataFrame(dataFrame: DataFrame, schema: StructType): DataFrame = {
    val fieldNames = schema.fieldNames
    val selectColumns = fieldNames.map(fieldName => col(fieldName))
    dataFrame.select(selectColumns: _*)
  }

  override def write(df: DataFrame): Int = {
    val table: String = config.getStringSafely( HiveConnectorConfig.table)
    val options = config.getStringMapSafely(HiveConnectorConfig.options)
    val partitionBy = config.getStringListSafely(HiveConnectorConfig.partition_by)
    val partitionSpec=config.getStringSafely(HiveConnectorConfig.partition_spec)
    val writeMode = config.getString( HiveConnectorConfig.mode, "append")

    var value = 0


    //对iceberg表暂不支持自动建表。
    val tableExists = SparkCatalogUtils.tableExists(table)

    var format="hive"
    if(tableExists){
      val provider=HiveUtils.getProvider(table)
      format= if ( provider.equals("iceberg") )"iceberg" else "hive"
      log.warn(s"table : $table ,provider :$provider ,format : $format")
    }
    var dfToWrite=df
    var dfWriter = dfToWrite.write.mode(writeMode).options(options).format(format)

    if(dfToWrite.schema.isEmpty){
      log.info("input is an empty dataframe,no need to write")
      return value
    }

    val recordsWrittenListener=new SparkListener(){
      override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
        if(taskEnd.reason==Success)
        synchronized {
          value = (taskEnd.taskMetrics.outputMetrics.recordsWritten+value).toInt
        }
      }
    }

    spark.sparkContext.addSparkListener(recordsWrittenListener)

    try {
      if (!tableExists) {
        log.warn(s"saveAsTable:$table")
        dfWriter.partitionBy(partitionBy: _*).saveAsTable(s"$table")
      } else if(partitionSpec.nonEmpty){
         //user spark sql insert overwrite
        val partitionSpecCols= partitionSpec.split(","
        ).map(_.split("=").head.trim)
        val fieldsWithoutPartition=spark.read.table(table).schema.fieldNames.filterNot(
          partitionSpecCols.contains(_))
        dfToWrite=df.select(fieldsWithoutPartition.map(col(_)):_*)
        val tmpViewName=config.getString("name")
        dfToWrite.createOrReplaceTempView(tmpViewName)
        val sqlWriteMode=if (writeMode.equalsIgnoreCase("OVERWRITE") ) writeMode else "into"
        val sql=
          s"""
            | INSERT $sqlWriteMode $table
            | PARTITION ( $partitionSpec )
            | SELECT * FROM $tmpViewName
            |
            |""".stripMargin
        spark.sql(sql)
      }
      else
      {
        log.warn(s"insertInto:$table")
        dfToWrite =reorderDataFrame(df,spark.read.table(table).schema)
          // 由于static mode不安全，可能非预期的删除历史数据，这里强制dynamic。
          spark.conf.set(SQLConf.PARTITION_OVERWRITE_MODE.key, "dynamic")
        if(format.equals("iceberg"))
         {
            //iceberg表的partition_cols需要为null赋予默认值。
            // TODO 对于非string的partition字段，这里可能会报错。
           val (partition_cols,_)=IcebergUtils.getPartitionSpecAndLocation(table)
           if(partition_cols.nonEmpty){
             partition_cols.split(",").foreach{
               partition_col=>
                 dfToWrite=dfToWrite.withColumn(partition_col,coalesce(col(partition_col),lit("_default_partition_value")))
             }
           }
           def putIfAbsent(kv:mutable.Map[String,String], key:String, value:String):Unit ={
             if(!kv.contains(key))
             {
               kv.put(key, value)
             }
           }
           //iceberg表需要增加一些option。默认情况下，不开启fanout writer会引起iceberg全局排序，这和hive默认的写入表现不同，会使写入性能下降。
          putIfAbsent(options,"distribution-mode","none")
          putIfAbsent(options,"fanout-enabled","true")
         }

        dfWriter = dfToWrite.write.mode(writeMode).options(options).format(format)
        if(format.equals("iceberg") && writeMode.equalsIgnoreCase("overwrite")){
          dfToWrite.writeTo(s"$table").options(options).overwritePartitions()
        }else{
          dfWriter.insertInto(s"$table")
        }
      }
    } catch {
      case t: Throwable =>
        log.error(config.getString("name")+" : write df failed!")
        throw t
    }
    spark.sparkContext.removeSparkListener(recordsWrittenListener)
    log.warn(s"$value records written successfully by task ${config.getString("name")} ")
    value
  }

}

