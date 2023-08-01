package io.github.seabow.datax.core.pipeline.processor

import io.github.seabow.datax.common.ConfigUtils._
import io.github.seabow.datax.core.pipeline.Processor
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.apache.spark.sql.utils.AutoProjectionUtils.reorderDataFrame

import scala.collection.mutable.ListBuffer
object AutoProjectionProcessorConfig{
  val schema="schema"
  val table="table"
}
class AutoProjectionProcessor extends Processor{

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
