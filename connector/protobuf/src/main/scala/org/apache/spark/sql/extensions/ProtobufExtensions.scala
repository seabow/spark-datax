package org.apache.spark.sql.extensions

import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.{BloomFilterMightContain, Expression, ExpressionInfo}
import org.apache.spark.sql.protobuf.EvolutionProtobufDataToCatalyst
import org.apache.spark.sql.protobuf.udfs.{GetJsonObjectExtension, GetProtobufJsonObject}

class ProtobufExtensions extends (SparkSessionExtensions=>Unit){
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectFunction(
      FunctionIdentifier("get_protobuf_json_object"),
      new ExpressionInfo(
        classOf[GetProtobufJsonObject].getName, "get_protobuf_json_object"),
      (expressions: Seq[Expression]) => {
        GetProtobufJsonObject(expressions(0),expressions(1),expressions(2),expressions(3))
      }
    )
    extensions.injectFunction(
      FunctionIdentifier("get_json_object"),
      new ExpressionInfo(
        classOf[GetJsonObjectExtension].getName, "get_json_object"),
      (expressions: Seq[Expression]) => {
        GetJsonObjectExtension(expressions(0),expressions(1))
      }
    )
  }
}
