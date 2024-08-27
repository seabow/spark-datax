package org.apache.spark.sql.core.udfs

import io.github.seabow.datax.common.HdfsUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, ExpressionDescription, UnaryExpression}
import org.apache.spark.sql.types.{AbstractDataType, BooleanType, DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String

@ExpressionDescription(
  usage = "_FUNC_(bool) - if bool is false then throw exception",
  examples =
    """
    Examples:
      > SELECT _FUNC_('bool');
  """,
  group = "json_funcs")
case class Assert(child: Expression)
  extends UnaryExpression with ExpectsInputTypes with CodegenFallback with Logging {
  override def inputTypes: Seq[AbstractDataType] =  Seq(BooleanType)

  override def nullSafeEval(input: Any): Any = {
    val expect=input.asInstanceOf[Boolean]
    if(!expect){
      throw new IllegalStateException("Unexpected situation")
    }
    expect
  }
  override def dataType: DataType = BooleanType
}
