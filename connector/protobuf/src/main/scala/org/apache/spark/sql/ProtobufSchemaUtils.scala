package org.apache.spark.sql

import org.apache.spark.internal.Logging
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.protobuf.utils.{ProtobufUtils, SchemaConverters}
import org.apache.spark.sql.types.StructType.fieldsMap
import org.apache.spark.sql.types.{ArrayType, DataType, DayTimeIntervalType, DecimalType, MapType, StringType, StructField, StructType, UserDefinedType, YearMonthIntervalType}

import scala.collection.mutable
import scala.util.control.NonFatal

object ProtobufSchemaUtils extends Logging{

  //输入proto_dir和msg_type，输出schema
  def getProtobufSchema(descriptorPath:String,msgName:String):String={
    ProtobufUtils.readDescriptorFileContent(descriptorPath)
    val descriptor= ProtobufUtils.buildDescriptor(ProtobufUtils.readDescriptorFileContent(descriptorPath),msgName)
    SchemaConverters.toSqlType(descriptor).dataType.sql
  }

  def getProtobuSchema(msgName:String):String={
    val descriptor=  ProtobufUtils.buildDescriptorFromJavaClass(msgName)
    SchemaConverters.toSqlType(descriptor).dataType.sql
  }

  def merge(left: DataType, right: DataType): DataType =
    mergeInternal(left, right, (s1: StructType, s2: StructType) => {
      val leftFields = s1.fields
      val rightFields = s2.fields
      val newFields = mutable.ArrayBuffer.empty[StructField]

      val rightMapped = fieldsMap(rightFields)
      leftFields.foreach {
        case leftField @ StructField(leftName, leftType, leftNullable, _) =>
          rightMapped.get(leftName)
            .map { case rightField @ StructField(rightName, rightType, rightNullable, _) =>
              try {
                leftField.copy(
                  dataType = merge(leftType, rightType),
                  nullable = leftNullable || rightNullable)
              } catch {
                case NonFatal(e) =>
//                  throw QueryExecutionErrors.failedMergingFieldsError(leftName, rightName, e)
                  log.warn(s"Failed to merge fields '$leftName' and '$rightName'. ${e.getMessage},Use string type instead")
                  leftField.copy(
                    dataType = StringType,
                    nullable = leftNullable || rightNullable)
              }
            }
            .orElse {
              Some(leftField)
            }
            .foreach(newFields += _)
      }

      val leftMapped = fieldsMap(leftFields)
      rightFields
        .filterNot(f => leftMapped.get(f.name).nonEmpty)
        .foreach { f =>
          newFields += f
        }

      StructType(newFields.toSeq)
    })

  private def mergeInternal(
                             left: DataType,
                             right: DataType,
                             mergeStruct: (StructType, StructType) => StructType): DataType =
    (left, right) match {
      case (ArrayType(leftElementType, leftContainsNull),
      ArrayType(rightElementType, rightContainsNull)) =>
        ArrayType(
          mergeInternal(leftElementType, rightElementType, mergeStruct),
          leftContainsNull || rightContainsNull)

      case (MapType(leftKeyType, leftValueType, leftContainsNull),
      MapType(rightKeyType, rightValueType, rightContainsNull)) =>
        MapType(
          mergeInternal(leftKeyType, rightKeyType, mergeStruct),
          mergeInternal(leftValueType, rightValueType, mergeStruct),
          leftContainsNull || rightContainsNull)

      case (s1: StructType, s2: StructType) => mergeStruct(s1, s2)

      case (DecimalType.Fixed(leftPrecision, leftScale),
      DecimalType.Fixed(rightPrecision, rightScale)) =>
        if (leftScale == rightScale) {
          DecimalType(leftPrecision.max(rightPrecision), leftScale)
        } else {
          throw QueryExecutionErrors.cannotMergeDecimalTypesWithIncompatibleScaleError(
            leftScale, rightScale)
        }

      case (leftUdt: UserDefinedType[_], rightUdt: UserDefinedType[_])
        if leftUdt.userClass == rightUdt.userClass => leftUdt

      case (YearMonthIntervalType(lstart, lend), YearMonthIntervalType(rstart, rend)) =>
        YearMonthIntervalType(Math.min(lstart, rstart).toByte, Math.max(lend, rend).toByte)

      case (DayTimeIntervalType(lstart, lend), DayTimeIntervalType(rstart, rend)) =>
        DayTimeIntervalType(Math.min(lstart, rstart).toByte, Math.max(lend, rend).toByte)

      case (leftType, rightType) if leftType == rightType =>
        leftType

      case _ =>
        throw QueryExecutionErrors.cannotMergeIncompatibleDataTypesError(left, right)
    }
}
