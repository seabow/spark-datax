package org.apache.spark.sql.utils

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{array, lit, to_json}
import org.apache.spark.sql.types.{ArrayType, AtomicType, DataType, MapType, StringType, StructType}

object AutoProjectionUtils {
  def reorderDataFrame(dataFrame: DataFrame, toSchema: StructType): DataFrame = {
    val fromSchema=dataFrame.schema
    val fromFieldNames=fromSchema.fieldNames
    val toFieldNames = toSchema.fieldNames
    val selectColumns = toFieldNames.map { toFieldName =>
      //如下应该改成
      val fromFieldNameOption = fromFieldNames.find(_.equalsIgnoreCase(toFieldName))
      if (fromFieldNameOption.isDefined) {
        val fromFieldName=fromFieldNameOption.get
        (fromSchema(fromFieldName).dataType, toSchema(toFieldName).dataType) match {
          case (from: DataType, to: DataType) if from.equals(to) =>
            dataFrame(fromFieldName).as(toFieldName)
          case (from: StructType, StringType) =>
            to_json(dataFrame(fromFieldName)).as(toFieldName)
          case (ArrayType(elementType, _), StringType) =>
            to_json(dataFrame(fromFieldName)).as(toFieldName)
          case (MapType(k, v, _), to: StringType) =>
            to_json(dataFrame(fromFieldName)).as(toFieldName)
          case (from:AtomicType,ArrayType(elementType,_))=>
            array(dataFrame(fromFieldName)).cast(toSchema(toFieldName).dataType).as(toFieldName)
          case _ =>
            dataFrame(fromFieldName).cast(toSchema(toFieldName).dataType).as(toFieldName)
        }
      } else {
        lit(null).as(toFieldName)
      }
    }
    dataFrame.select(selectColumns: _*)
  }
}
