package io.github.seabow.datax.core.pipeline.connector

import io.github.seabow.datax.common.ConfigUtils.ImplicitConfigUtils
import io.github.seabow.datax.core.pipeline.Connector
import org.apache.spark.Success
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType

object HiveConnectorConfig{
  def table = "table"
  def partition_by = "partition_by"
  //partition_spec as: partition_col_1='partition_value_1',partition_col_2='partition_value_2'
  def partition_spec = "partition_spec"
  def partition_cols = "partition_cols"
  def mode = "mode"
  def options = "options"
  def format="format"
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
    val format = config.getString(HiveConnectorConfig.format, "hive" )
    val partition_cols=config.getStringSafely(HiveConnectorConfig.partition_cols)

    var value = 0

    //对iceberg表暂不支持自动建表。
    var tableExists = format.equals("iceberg") || spark.catalog.tableExists(s"$table")
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
        //默认partitionOverwriteMode=dynamic
        if(!options.contains("partitionOverwriteMode"))
          {
            options.put("partitionOverwriteMode", "dynamic")
          }
          if(partition_cols.nonEmpty){
            partition_cols.split(",").foreach{
              partition_col=>
              dfToWrite=dfToWrite.withColumn(partition_col,coalesce(col(partition_col),lit("_default_partition_value")))
            }
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
