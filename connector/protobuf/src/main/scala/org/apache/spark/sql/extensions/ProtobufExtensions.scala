package org.apache.spark.sql.extensions

import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.{BloomFilterMightContain, Expression, ExpressionInfo}
import org.apache.spark.sql.protobuf.EvolutionProtobufDataToCatalyst
import org.apache.spark.sql.protobuf.udfs.GetProtobufJsonObject

class ProtobufExtensions extends (SparkSessionExtensions=>Unit){
  override def apply(extensions: SparkSessionExtensions): Unit = {
    val functionIdentifier= FunctionIdentifier("get_protobuf_json_object")
    val builder=(expressions: Seq[Expression]) => {
      GetProtobufJsonObject(expressions(0),expressions(1),expressions(2),expressions(3))
    }
    val info = new ExpressionInfo(
      classOf[GetProtobufJsonObject].getName, functionIdentifier.funcName)
    extensions.injectFunction(
      functionIdentifier,info,
      builder
    )
  }
}
