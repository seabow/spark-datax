package io.github.seabow.datax.core.pipeline.connector

import io.github.seabow.datax.common.ConfigUtils.ImplicitConfigUtils
import io.github.seabow.datax.core.pipeline.Connector
import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType

object HiveConnectorConfig{
  def table = "table"
  def partition_by = "partition_by"
  //partition_spec as: partition_col_1='partition_value_1',partition_col_2='partition_value_2'
  def partition_spec = "partition_spec"
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

    var value = 1

    var tableExists = spark.catalog.tableExists(s"$table")

    var dfToWrite=df
    var dfWriter = dfToWrite.write.mode(writeMode).options(options).format(format)


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
        val sql=
          s"""
            | INSERT $writeMode $table
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
        dfWriter = dfToWrite.write.mode(writeMode).options(options).format(format)
        dfWriter.insertInto(s"$table")
      }

    } catch {
      case t: Throwable =>
        log.error("write df failed", t)
        value = 0
    }
    value
  }

}
