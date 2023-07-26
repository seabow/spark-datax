package io.github.seabow.datax.core.pipeline.connector

import io.github.seabow.datax.common.ConfigUtils.ImplicitConfigUtils
import io.github.seabow.datax.core.pipeline.Connector
import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object HiveConnectorConfig{
  def table = "table"
  def partition_by = "partition_by"
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
    val dataSchema=dataFrame.schema
    val fieldNames = schema.fieldNames
    val selectColumns = fieldNames.map(fieldName =>
      if(dataSchema.fieldNames.contains(fieldName))
        {
          (dataSchema(fieldName).dataType,schema(fieldName).dataType) match {
            case (from:DataType,to:DataType) if from.equals(to)=>
              col(fieldName)
            case (from:StructType,StringType)=>
              to_json(col(fieldName))
            case (ArrayType(elementType, _) ,StringType)=>
              to_json(col(fieldName))
            case (MapType(k,v,_),to:StringType)=>
              to_json(col(fieldName))
            case _=>
              col(fieldName).cast(schema(fieldName).dataType)
          }
        }else{
         lit(null)
        }
      )
    dataFrame.select(selectColumns: _*)
  }

  override def write(df: DataFrame): Int = {
    val table: String = config.getStringSafely( HiveConnectorConfig.table)
    val options = config.getStringMapSafely(HiveConnectorConfig.options)
    val partitionBy = config.getStringListSafely(HiveConnectorConfig.partition_by)
    val writeMode = config.getString( HiveConnectorConfig.mode, "append")
    val format = config.getString(HiveConnectorConfig.format, "hive" )

    var value = 1

    var tableExists = spark.catalog.tableExists(s"$table")

    var dfToWrite=df

    if(tableExists)
      {
        dfToWrite =reorderDataFrame(df,spark.read.table(table).schema)
      }

    var dfWriter = dfToWrite.write.mode(writeMode).options(options).format(format)


    try {
      if (!tableExists) {
        log.warn(s"saveAsTable:$table")
        dfWriter.partitionBy(partitionBy: _*).saveAsTable(s"$table")
      } else {
        log.warn(s"insertInto:$table")
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
