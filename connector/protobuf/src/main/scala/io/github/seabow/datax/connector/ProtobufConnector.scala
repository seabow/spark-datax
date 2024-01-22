package io.github.seabow.datax.connector

import io.github.seabow.datax.core.pipeline.Connector
import org.apache.spark.sql.DataFrame
import io.github.seabow.datax.common.ConfigUtils._
import io.github.seabow.datax.common.SparkUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.protobuf.functions.from_protobuf

import scala.collection.JavaConverters._

object ProtobufConnectorConfig {
  val input = "input"
  val from_col = "from_col"
  val to_col = "to_col"
  val descriptor_path = "descriptor_path"
  val descriptor_path_col = "descriptor_path_col"
  val schema = "schema"
  val schema_file = "schema_file"
  val schema_table_col = "schema_table_col"
  val msg_name = "msg_name"
  val msg_name_col = "msg_name_col"
  val options = "options"
}

class ProtobufConnector extends Connector {
  override def shortName(): String = "protobuf"

  override def read(): DataFrame = {
    val input = config.getStringSafely(ProtobufConnectorConfig.input)
    val from_col = config.getStringSafely(ProtobufConnectorConfig.from_col)
    val to_col = config.getStringSafely(ProtobufConnectorConfig.to_col)
    val descriptor_path = config.getStringSafely(ProtobufConnectorConfig.descriptor_path)
    val descriptor_path_col = config.getStringSafely(ProtobufConnectorConfig.descriptor_path_col)
    val msg_name = config.getStringSafely(ProtobufConnectorConfig.msg_name)
    val msg_name_col = config.getStringSafely(ProtobufConnectorConfig.msg_name_col)
    val options = config.getStringMapSafely(ProtobufConnectorConfig.options)
    val schema = config.getStringSafely(ProtobufConnectorConfig.schema)
    val schema_file = config.getStringSafely(ProtobufConnectorConfig.schema_file)
    val schema_table_col = config.getStringSafely(ProtobufConnectorConfig.schema_table_col)

    val structSchema =
      if (schema_table_col.nonEmpty) {
        val tableAndCol = schema_table_col.split(",")
        val table = tableAndCol.head
        val col = tableAndCol.last
        spark.read.table(table).schema(col).dataType.sql
      } else if (schema_file.nonEmpty) {
        SparkUtils.getFileContent(schema_file)
      } else {
        schema
      }

    assert(input.nonEmpty)
    val inputDF = job.outputMap(input)
    if (descriptor_path.nonEmpty && descriptor_path_col.isEmpty) {
      inputDF.withColumn(to_col, from_protobuf(col(from_col), msg_name, descriptor_path, options.asJava))
    }else if (descriptor_path_col.nonEmpty && msg_name_col.nonEmpty){
      inputDF.withColumn(to_col, from_protobuf(col(from_col), col(descriptor_path_col), col(msg_name_col), structSchema,options.asJava))
    }else if (descriptor_path_col.nonEmpty && msg_name_col.isEmpty) {
      inputDF.withColumn(to_col, from_protobuf(col(from_col), col(descriptor_path_col), msg_name, structSchema, options.asJava))
    } else {
      //msg_name only
      if(structSchema.isEmpty)
        {
          inputDF.withColumn(to_col, from_protobuf(col(from_col), msg_name, options.asJava))
        }
        else{
        inputDF.withColumn(to_col, from_protobuf(col(from_col), msg_name, options.asJava,structSchema))
      }
    }
  }
}
