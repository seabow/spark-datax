package org.apache.spark.sql.protobuf.udfs

import com.google.protobuf.Descriptors.{Descriptor, FieldDescriptor}
import com.google.protobuf.UncheckedDynamicMessage
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, BindReferences, ExpectsInputTypes, Expression, ExpressionDescription, GetJsonObject, Literal, QuaternaryExpression}
import org.apache.spark.sql.errors.Implicits.QueryComiplationErrorsImplicit
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.protobuf.utils.ProtobufUtils
import org.apache.spark.sql.types.{AbstractDataType, BinaryType, DataType, StringType, StructType}
import org.apache.spark.unsafe.types.UTF8String

import java.util.Locale
import scala.collection.JavaConverters._


@ExpressionDescription(
  usage = "_FUNC_(protobuf_bytes, path) - Extracts a json object from protobuf `path`.",
  examples =
    """
    Examples:
      > SELECT _FUNC_('array[bytes]', '$.a');
       b
  """,
  group = "json_funcs")
case class GetJsonObjectExtension(protobuf_or_json: Expression, path: Expression)
  extends BinaryExpression with CodegenFallback with Logging {
  override def left: Expression = protobuf_or_json
  override def right: Expression = path

  override protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): GetJsonObjectExtension =
    copy(protobuf_or_json = newLeft, path = newRight)
  override def dataType: DataType = StringType

  override def nullable: Boolean = true

  override def prettyName: String = "get_json_object"

  val get_json_object_expr= GetJsonObject(protobuf_or_json,path)

  override def eval(input: InternalRow): Any = {
    try {
      protobuf_or_json.dataType match {
        case StructType(fields)=>
        val struct= protobuf_or_json.eval(input).asInstanceOf[InternalRow]
          val pb=struct.get(0,BinaryType)
          val msgType=struct.get(1,StringType).asInstanceOf[UTF8String].toString
          val descriptor_path=struct.get(2,StringType).asInstanceOf[UTF8String].toString
          GetProtobufJsonObject(Literal(pb),path, Literal(msgType), Literal(descriptor_path)).eval(input)
        case StringType=>
          get_json_object_expr.eval(input)
        case _=> null
      }
    } catch {
      case e:Throwable=>
        e.printStackTrace
        null
    }
  }
}
