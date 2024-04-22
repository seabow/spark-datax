package org.apache.spark.sql.protobuf.udfs

import com.google.protobuf.Descriptors.{Descriptor, FieldDescriptor}
import com.google.protobuf.DynamicMessage
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, ExpressionDescription, GetJsonObject, Literal, QuaternaryExpression}
import org.apache.spark.sql.errors.Implicits.QueryComiplationErrorsImplicit
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.protobuf.utils.{ProtobufJsonFormat, ProtobufOptions, ProtobufUtils}
import org.apache.spark.sql.types.{AbstractDataType, BinaryType, DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String

import java.util.Locale
import scala.collection.JavaConverters._
import scala.collection.concurrent.TrieMap

object GetProtobufJsonObject {
  //(msgName,descFilePath)
  @transient private lazy val messageDescriptorMap =
  TrieMap.empty[(String, String), Descriptor]
  @transient private lazy val messageCahceMap =
    TrieMap.empty[InternalRow, DynamicMessage]
  private lazy val protobufOptions = ProtobufOptions()
  //we don't parse any type for protobuf
  private lazy val jsonPrinter = {
    var jsonPrinter =
      ProtobufJsonFormat.printer
        .omittingInsignificantWhitespace()
        .preservingProtoFieldNames()
    if (protobufOptions.castEnumAsInt) {
      jsonPrinter = jsonPrinter.printingEnumsAsInts()
    }
    if (protobufOptions.emitDefaultValues) {
      jsonPrinter = jsonPrinter.includingDefaultValueFields()
    }
    jsonPrinter
  }
}

@ExpressionDescription(
  usage = "_FUNC_(protobuf_bytes, path,msgName,descFilePath) - Extracts a json object from protobuf `path`.",
  examples =
    """
    Examples:
      > SELECT _FUNC_('array[bytes]', '$.a','Student','hdfs:///path/to/descriptorFile');
       b
  """,
  group = "json_funcs")
case class GetProtobufJsonObject(protobuf_bytes: Expression, path: Expression, msgName: Expression, descFilePath: Expression)
  extends QuaternaryExpression with ExpectsInputTypes with CodegenFallback with Logging {
  override def inputTypes: Seq[AbstractDataType] = Seq(BinaryType, StringType, StringType, StringType)

  override def first: Expression = protobuf_bytes

  override def second: Expression = path

  override def third: Expression = msgName

  override def fourth: Expression = descFilePath

  override protected def withNewChildrenInternal(newFirst: Expression, newSecond: Expression, newThird: Expression, newFourth: Expression): Expression = {
    copy(protobuf_bytes = newFirst, path = newSecond, msgName = newThird, descFilePath = newFourth)
  }

  override def dataType: DataType = StringType

  override def nullable: Boolean = true

  override def prettyName: String = "get_protobuf_json_object"

  //we don't parse any type for protobuf
  private lazy val currentJsonPrinter = GetProtobufJsonObject.jsonPrinter

  import GetProtobufJsonObject._

  private def loadNewProtoVersion(msgName: String, protoDescFile: String): Unit = {
    val messageDescriptorMapKey = (msgName, protoDescFile)
    if (!messageDescriptorMap.contains(messageDescriptorMapKey)) {
      val binaryFileDescriptorSet: Option[Array[Byte]] = if (protoDescFile.nonEmpty) {
        Some(ProtobufUtils.readDescriptorFileContent(protoDescFile))
      } else {
        None
      }
      val messageDescriptor = ProtobufUtils.buildDescriptor(msgName, binaryFileDescriptorSet)
      messageDescriptorMap.put(messageDescriptorMapKey,
        messageDescriptor)
    }
  }

  private def getMessageDescriptor(msgName: String, descFilePath: String): Descriptor = {
    val messageDescriptorMapKey = if (msgName.contains(".")) {
      (msgName, "")
    } else {
      (msgName, descFilePath)
    }
    if (!messageDescriptorMap.contains(messageDescriptorMapKey)) {
      log.warn(s"load descriptor:$msgName,$descFilePath")
      loadNewProtoVersion(messageDescriptorMapKey._1, messageDescriptorMapKey._2)
    }
    messageDescriptorMap(messageDescriptorMapKey)
  }

  override def eval(input: InternalRow): Any = {
    try {
      evalInternal(input)
    } catch {
      case e: Throwable => null
    }
  }

  def getField(fieldName: String, message: DynamicMessage): Option[(FieldDescriptor, AnyRef)] = {
    val fieldMap = message.getDescriptorForType.getFields.asScala
      .groupBy(_.getName.toLowerCase(Locale.ROOT))
      .mapValues(_.toSeq)

    def getFieldByName(name: String): Option[FieldDescriptor] = {
      // get candidates, ignoring case of field name
      val candidates = fieldMap.getOrElse(name.toLowerCase(Locale.ROOT), Seq.empty)
      // search candidates, taking into account case sensitivity settings
      candidates.filter(f => SQLConf.get.resolver(f.getName(), name)) match {
        case Seq(protoField) => Some(protoField)
        case Seq() => None
        case matches =>
          throw QueryCompilationErrors.protobufFieldMatchError(
            name,
            fieldName,
            s"${matches.size}",
            matches.map(_.getName()).mkString("[", ", ", "]"))
      }
    }

    val fieldOption = getFieldByName(fieldName)
    if (fieldOption.isEmpty) {
      return None
    }
    val field = fieldOption.get
    if (message.getAllFields.containsKey(field)) {
      return Some((field, message.getField(field)))
    }
    if (protobufOptions.emitDefaultValues) {
      if (field.isOptional) {
        if ((field.getJavaType eq FieldDescriptor.JavaType.MESSAGE) && !message.hasField(field)) {
          // Always skip empty optional message fields. If not we will recurse indefinitely if
          // a message has itself as a sub-field.
          return None
        }
        val oneof = field.getContainingOneof
        if (oneof != null && !message.hasField(field)) {
          // Skip all oneof fields except the one that is actually set
          return None
        }
        return Some((field, message.getField(field)))
      }
    }
    None
  }

  private def evalInternal(input: InternalRow): Any = {
    val protoBinary = protobuf_bytes.eval(input).asInstanceOf[Array[Byte]]
    val pathStr = path.eval(input).asInstanceOf[UTF8String].toString
    val msgNameStr = msgName.eval(input).asInstanceOf[UTF8String].toString
    val descFilePathStr = descFilePath.eval(input).asInstanceOf[UTF8String].toString
    val descriptor = getMessageDescriptor(msgNameStr, descFilePathStr)
    var dynamicMessage = if (messageCahceMap.contains(input)) {
      messageCahceMap(input)
    } else {
      val result = DynamicMessage.parseFrom(descriptor, protoBinary)
      messageCahceMap.clear()
      messageCahceMap.put(input, result)
      result
    }
    val pathSeq = pathStr.split("\\.")
    assert(pathSeq.nonEmpty && pathSeq.head.equals("$"))
    val printToIndex = if (pathSeq.filter(_.contains("[")).size > 0) {
      pathSeq.zipWithIndex.filter(_._1.contains("[")).map(_._2).min
    } else {
      pathSeq.size - 1
    }
    var index = 1
    while (index < printToIndex) {
      val fieldOption = getField(pathSeq(index), dynamicMessage)
      if (fieldOption.isEmpty) {
        return null
      }
      try {
        dynamicMessage = fieldOption.get._2.asInstanceOf[DynamicMessage]
      } catch {
        case e: Throwable =>
          log.warn(s"can not cast field ${pathSeq.slice(0, index + 1).mkString(".")} as a message")
          return null
      }
      index += 1
    }
    val leafPathSeq = pathSeq.slice(printToIndex, pathSeq.size)
    if (leafPathSeq.length == 1 && !leafPathSeq.head.contains("[")) {
      val fieldOption = getField(leafPathSeq.head, dynamicMessage)
      if (fieldOption.isEmpty) {
        return null
      } else {
        UTF8String.fromString(currentJsonPrinter.printFieldWithoutKey(fieldOption.get._1, fieldOption.get._2))
      }
    } else {
      GetJsonObject(Literal(currentJsonPrinter.print(dynamicMessage)), Literal("$." + leafPathSeq.mkString("."))).eval(input)
    }
  }
}
