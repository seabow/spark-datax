package org.apache.spark.sql.core.udfs

import io.github.seabow.datax.common.HdfsUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, ExpressionDescription, UnaryExpression}
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types.{AbstractDataType, ArrayType, BooleanType, DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String

@ExpressionDescription(
  usage = "_FUNC_(path) - if path exists return true,else return false",
  examples =
    """
    Examples:
      > SELECT _FUNC_('path');
       b
  """,
  group = "json_funcs")
case class PathExists(child: Expression)
  extends UnaryExpression with ExpectsInputTypes with CodegenFallback with Logging {
  override def inputTypes: Seq[AbstractDataType] =  Seq(StringType)

  override def nullSafeEval(input: Any): Any = {
    val path=input.asInstanceOf[UTF8String].toString
     HdfsUtils.exist(path)
  }

  override def dataType: DataType = BooleanType
}
