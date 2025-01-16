package org.apache.spark.sql.protobuf.udfs

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.gson.Gson
import com.google.protobuf.Descriptors.{Descriptor, FieldDescriptor}
import com.google.protobuf.{UncheckedDynamicMessage, ProtobufStatsUtils}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, ExpressionDescription, GetJsonObject, Literal, QuaternaryExpression}
import org.apache.spark.sql.errors.Implicits.QueryComiplationErrorsImplicit
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.protobuf.utils.ProtobufUtils
import org.apache.spark.sql.types.{AbstractDataType, BinaryType, DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String

import java.util.Locale
import scala.collection.JavaConverters._
import scala.collection.concurrent.TrieMap

object StatsProtobuf {
  //(msgName,descFilePath)
  @transient private lazy val descriptorBytesMap =
  TrieMap.empty[ String, Option[Array[Byte]]]
  @transient private lazy val messageDescriptorMap =
    TrieMap.empty[(String, String), Option[Descriptor]]

  @transient private lazy val messageCahceMap =
    TrieMap.empty[InternalRow, UncheckedDynamicMessage]
}
@ExpressionDescription(
  usage = "_FUNC_(protobuf_bytes, path,msgName,descFilePath) - stats protobuf size in bytes.",
  examples =
    """
    Examples:
      > SELECT _FUNC_('array[bytes]', '$.a','Student','hdfs:///path/to/descriptorFile');
       b
  """,
  group = "json_funcs")
case class StatsProtobuf(protobuf_bytes: Expression, path: Expression, msgName: Expression, descFilePath: Expression)
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

  override def prettyName: String = "stats_protobuf"

  @transient lazy val gson=new Gson()

  import StatsProtobuf._

  private def loadNewProtoVersion(msgName: String, protoDescFile: String): Unit = {
    val messageDescriptorMapKey = (msgName, protoDescFile)
    if (!messageDescriptorMap.contains(messageDescriptorMapKey)) {
      val binaryFileDescriptorSet: Option[Array[Byte]] = if (protoDescFile.nonEmpty) {
        if(descriptorBytesMap.contains(protoDescFile)){
          descriptorBytesMap(protoDescFile)
        }else{
          val bytesOption=try{
            Some(ProtobufUtils.readDescriptorFileContent(protoDescFile))
          }catch {
            case e: Throwable=>
              None
          }
          descriptorBytesMap.put(protoDescFile, bytesOption)
          bytesOption
        }
      } else {
        None
      }
      val messageDescriptor =  try{
        Some( ProtobufUtils.buildDescriptor(msgName, binaryFileDescriptorSet))
      }catch {
        case e:Throwable=>
          e.printStackTrace()
          None
      }
      messageDescriptorMap.put(messageDescriptorMapKey,
        messageDescriptor)
    }
  }

  private def getMessageDescriptor(msgName: String, descFilePath: String): Option[Descriptor] = {
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
      case e: Throwable =>
        throw e
    }
  }

  def fieldSetted(message:UncheckedDynamicMessage, field:FieldDescriptor): Boolean ={
    (!field.isRepeated &&  message.hasField(field) ) || message.getRepeatedFieldCount(field)>0
  }

  def getField(fieldName: String, message: UncheckedDynamicMessage): Option[(FieldDescriptor, AnyRef)] = {
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
    if (fieldSetted(message, field)) {
      return Some((field, message.getField(field)))
    }
    else {
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
    if(descriptor.isEmpty){
      return null
    }
    var dynamicMessage = if (messageCahceMap.contains(input)) {
      messageCahceMap(input)
    } else {
      val result = UncheckedDynamicMessage.parseFrom(descriptor.get, protoBinary)
      messageCahceMap.clear()
      messageCahceMap.put(input, result)
      result
    }
    val pathSeq = pathStr.split("\\.")
    assert(pathSeq.nonEmpty && pathSeq.head.equals("$"))
    val printToIndex = Math.max(1,pathSeq.size - 1)
    var index = 1
    while (index < printToIndex) {
      val fieldOption = getField(pathSeq(index), dynamicMessage)
      if (fieldOption.isEmpty) {
        return null
      }
      try {
        dynamicMessage = fieldOption.get._2.asInstanceOf[UncheckedDynamicMessage]
      } catch {
        case e: Throwable =>
          return null
      }
      index += 1
    }
    val leafPathSeq = pathSeq.slice(printToIndex, pathSeq.size)
    def getMessageFieldSizeMap(dynamicMessage: UncheckedDynamicMessage):Map[String,Int]={
      dynamicMessage.getDescriptorForType.getFields.asScala.map{
        field=>
          val size= if(! fieldSetted(dynamicMessage,field)) 0 else {
            ProtobufStatsUtils.computeFieldSize(field,dynamicMessage.getField(field))
          }
          (field.getName,size)
      }.toMap
    }
   val resultMap= if(leafPathSeq.isEmpty){
      //统计dynamicMessage各个field的大小
       getMessageFieldSizeMap(dynamicMessage)
    }
    else{
      val fieldOption = getField(leafPathSeq.head, dynamicMessage)
      if (fieldOption.isEmpty) {
        return null
      } else {
        //dynamicMessage
        if (fieldOption.get._1.getJavaType eq FieldDescriptor.JavaType.MESSAGE)
          {
            getMessageFieldSizeMap(fieldOption.get._2.asInstanceOf[UncheckedDynamicMessage])
          }else{
           Map(fieldOption.get._1.getName->ProtobufStatsUtils.computeFieldSize(fieldOption.get._1,fieldOption.get._2))
        }
      }
    }
     UTF8String.fromString(gson.toJson(resultMap.asJava))
  }
}
