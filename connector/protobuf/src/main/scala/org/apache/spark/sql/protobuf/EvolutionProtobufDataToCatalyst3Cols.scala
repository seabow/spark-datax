/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.protobuf

import com.google.protobuf.Descriptors.Descriptor
import com.google.protobuf.{UncheckedDynamicMessage, TypeRegistry}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, SpecificInternalRow, TernaryExpression}
import org.apache.spark.sql.catalyst.util.{FailFastMode, ParseMode, PermissiveMode}
import org.apache.spark.sql.errors.Implicits.QueryComiplationErrorsImplicit
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.protobuf.utils.{ProtobufOptions, ProtobufUtils}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.control.NonFatal

private[sql] case class EvolutionProtobufDataToCatalyst3Cols(
                                                         first: Expression,
                                                         second: Expression,
                                                         third: Expression,
                                                         schema:DataType,
                                                         options: Map[String, String] = Map.empty)
  extends TernaryExpression
    with ExpectsInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(BinaryType,StringType,StringType)

  override lazy val dataType: DataType = schema

  override def nullable: Boolean = true

  private lazy val protobufOptions = ProtobufOptions(options)

  @transient private lazy val messageDescriptorMap =
    mutable.Map.empty[(String,String),Descriptor]

  @transient private lazy val fieldsNumbersMap =
    mutable.Map.empty[(String,String),Set[Int]]

  @transient private lazy val deserializerMap =
    mutable.Map.empty[(String,String),ProtobufDeserializer]

  private def loadNewProtoVersion(msgName:String,protoDescFile:String):Unit={
    val messageDescriptorMapKey=(msgName,protoDescFile)
    if(!messageDescriptorMap.contains(messageDescriptorMapKey)){
      val binaryFileDescriptorSet:Option[Array[Byte]]= if(protoDescFile.nonEmpty){
          Some(ProtobufUtils.readDescriptorFileContent(protoDescFile))
        }else{
          None
        }
      val messageDescriptor=ProtobufUtils.buildDescriptor( msgName,binaryFileDescriptorSet)
      messageDescriptorMap.put(messageDescriptorMapKey,
        messageDescriptor)
      fieldsNumbersMap.put(messageDescriptorMapKey, messageDescriptor.getFields.asScala.map(f => f.getNumber).toSet)
      val typeRegistry = binaryFileDescriptorSet match {
        case Some(descBytes) if protobufOptions.convertAnyFieldsToJson =>
          ProtobufUtils.buildTypeRegistry(descBytes) // This loads all the messages in the desc set.
        case None if protobufOptions.convertAnyFieldsToJson =>
          ProtobufUtils.buildTypeRegistry(messageDescriptor) // Loads only connected messages.
        case _ => TypeRegistry.getEmptyTypeRegistry // Default. Json conversion is not enabled.
      }
     val newDeserializer= new ProtobufDeserializer(
        messageDescriptor,
        dataType,
        typeRegistry = typeRegistry,
        emitDefaultValues = protobufOptions.emitDefaultValues,
        enumsAsInts = protobufOptions.castEnumAsInt
      )
      deserializerMap.put(messageDescriptorMapKey,newDeserializer)
    }
  }

  @transient private var result: UncheckedDynamicMessage = _

  @transient private lazy val parseMode: ParseMode = {
    val mode = protobufOptions.parseMode
    if (mode != PermissiveMode && mode != FailFastMode) {
      throw QueryCompilationErrors.parseModeUnsupportedError(prettyName, mode)
    }
    mode
  }

  @transient private lazy val nullResultRow: Any = dataType match {
    case st: StructType =>
      val resultRow = new SpecificInternalRow(st.map(_.dataType))
      for (i <- 0 until st.length) {
        resultRow.setNullAt(i)
      }
      resultRow

    case _ =>
      null
  }

  private def handleException(e: Throwable): Any = {
    parseMode match {
      case PermissiveMode =>
        nullResultRow
      case FailFastMode =>
        throw QueryExecutionErrors.malformedProtobufMessageDetectedInMessageParsingError(e)
      case _ =>
        throw QueryCompilationErrors.parseModeUnsupportedError(prettyName, parseMode)
    }
  }



  override def nullSafeEval(input1: Any,input2:Any,input3:Any): Any = {
    val binary = input1.asInstanceOf[Array[Byte]]
    val protoDescFile=input2.asInstanceOf[UTF8String].toString
    val messageName=input3.asInstanceOf[UTF8String].toString
    val messageDescriptorMapKey=if(messageName.contains(".")){
      (messageName,"")
    }else{
      (messageName,protoDescFile)
    }
    try {
      if(!messageDescriptorMap.contains(messageDescriptorMapKey)){
        loadNewProtoVersion(messageDescriptorMapKey._1,messageDescriptorMapKey._2)
      }
      result = UncheckedDynamicMessage.parseFrom(messageDescriptorMap(messageDescriptorMapKey), binary)
      // If the Java class is available, it is likely more efficient to parse with it than using
      // DynamicMessage. Can consider it in the future if parsing overhead is noticeable.

      result.getUnknownFields.asMap().keySet().asScala.find(fieldsNumbersMap(messageDescriptorMapKey).contains(_)) match {
        case Some(number) =>
          // Unknown fields contain a field with same number as a known field. Must be due to
          // mismatch of schema between writer and reader here.
          throw QueryCompilationErrors.protobufFieldTypeMismatchError(
            messageDescriptorMap(messageDescriptorMapKey).getFields.get(number).toString)
        case None =>
      }

      val deserialized = deserializerMap(messageDescriptorMapKey).deserialize(result)
      assert(
        deserialized.isDefined,
        "Protobuf deserializer cannot return an empty result because filters are not pushed down")
      deserialized.get
    } catch {
      // There could be multiple possible exceptions here, e.g. java.io.IOException,
      // ProtoRuntimeException, ArrayIndexOutOfBoundsException, etc.
      // To make it simple, catch all the exceptions here.
      case NonFatal(e) =>
        handleException(e)
    }
  }

  override def prettyName: String = "from_protobuf"

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val expr = ctx.addReferenceObj("this", this)
    nullSafeCodeGen(
      ctx,
      ev,
      (eval1,eval2,eval3) => {
        val result = ctx.freshName("result")
        val dt = CodeGenerator.boxedType(dataType)
        s"""
        $dt $result = ($dt) $expr.nullSafeEval($eval1,$eval2,$eval3);
        if ($result == null) {
          ${ev.isNull} = true;
        } else {
          ${ev.value} = $result;
        }
      """
      })
  }

  override protected def withNewChildrenInternal(newFirst: Expression,newSecond: Expression,newThird: Expression): EvolutionProtobufDataToCatalyst3Cols =
    copy(first = newFirst, second = newSecond,third=newThird)

}