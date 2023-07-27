package io.github.seabow.datax.core.pipeline.processor

import io.github.seabow.datax.common.ConfigUtils._
import io.github.seabow.datax.core.pipeline.Processor
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{lit, to_json}
import org.apache.spark.sql.types._

import scala.collection.mutable.ListBuffer
object AutoProjectionProcessorConfig{
  val schema="schema"
  val table="table"
}
class AutoProjectionProcessor extends Processor{
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
          case _ =>
            dataFrame(fromFieldName).cast(toSchema(toFieldName).dataType).as(toFieldName)
        }
      } else {
        lit(null).as(toFieldName)
      }
    }
    dataFrame.select(selectColumns: _*)
  }

  override def process(dfList: ListBuffer[DataFrame]): DataFrame ={
    val table=config.getStringSafely(AutoProjectionProcessorConfig.table)
    val schema=if(table.nonEmpty){
      spark.read.table(table).schema
    }else{
      StructType.fromDDL(config.getStringSafely(AutoProjectionProcessorConfig.schema))
    }
    assert(schema.nonEmpty)
    reorderDataFrame(dfList.head,schema)
  }

  override def shortName(): String = "auto_projection"
}
