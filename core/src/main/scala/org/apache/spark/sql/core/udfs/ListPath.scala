package org.apache.spark.sql.core.udfs

import io.github.seabow.datax.common.HdfsUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, ExpressionDescription, QuaternaryExpression, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

@ExpressionDescription(
  usage = "_FUNC_(path) - list sub path for dir",
  examples =
    """
    Examples:
      > SELECT _FUNC_('path');
       b
  """,
  group = "json_funcs")
case class ListPath(child: Expression)
  extends UnaryExpression with ExpectsInputTypes with CodegenFallback with Logging {
  override def inputTypes: Seq[AbstractDataType] =  Seq(StringType)

  override def eval(input: InternalRow): Any = {
    val value = child.eval(input)
    if (value == null) {
      null
    } else {
      val path=input.asInstanceOf[UTF8String].toString
      if(!HdfsUtils.exist(path)){
        return null
      }else{
        val subpaths=HdfsUtils.listStatus(path).map(_.getPath.toString).map(UTF8String.fromString)
        new GenericArrayData(subpaths.asInstanceOf[Array[Any]])
      }
    }
  }

  override def dataType: DataType = ArrayType(StringType, containsNull = false)
}