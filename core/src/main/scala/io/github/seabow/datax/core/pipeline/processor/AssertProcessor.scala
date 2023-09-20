package io.github.seabow.datax.core.pipeline.processor

import io.github.seabow.datax.common.ConfigUtils._
import io.github.seabow.datax.core.pipeline.Processor
import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame

import scala.collection.mutable.ListBuffer

class AssertProcessor extends Processor with Logging{
  override def process(dfList: ListBuffer[DataFrame]): DataFrame = {
     dfList.foreach{
       df=>
           assert(df.collect().map(_.getBoolean(0)).head)
     }
     spark.emptyDataFrame
  }

  override def shortName(): String = "assert"
}

