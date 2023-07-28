package io.github.seabow.datax.connector

import io.github.seabow.datax.core.pipeline.Connector
import org.apache.spark.sql.DataFrame
import io.github.seabow.datax.common.ConfigUtils._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.protobuf.functions.from_protobuf

import scala.collection.JavaConverters._

object ProtobufConnectorConfig{
  val input="input"
  val from_col="from_col"
  val to_col="to_col"
  val descriptor_path="descriptor_path"
  val msg_name="msg_name"
  val options="options"
}

class ProtobufConnector extends Connector{
  override def shortName(): String = "protobuf"

  override def read(): DataFrame = {
    val input=config.getStringSafely(ProtobufConnectorConfig.input)
    val from_col=config.getStringSafely(ProtobufConnectorConfig.from_col)
    val to_col=config.getStringSafely(ProtobufConnectorConfig.to_col)
    val descriptor_path=config.getStringSafely(ProtobufConnectorConfig.descriptor_path)
    val msg_name=config.getStringSafely(ProtobufConnectorConfig.msg_name)
    val options=config.getStringMapSafely(ProtobufConnectorConfig.options)
    assert(input.nonEmpty)
    val inputDF=job.outputMap(input)
    inputDF.withColumn(to_col,from_protobuf(col(from_col),msg_name,descriptor_path,options.asJava))
  }
}
