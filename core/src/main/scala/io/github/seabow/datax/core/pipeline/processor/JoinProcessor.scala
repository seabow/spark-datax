package io.github.seabow.datax.core.pipeline.processor

import io.github.seabow.datax.common.ConfigUtils._
import io.github.seabow.datax.core.pipeline.Processor
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

object JoinProcessorConfig {

  val joinCols = "join_cols"
  val joinMethod = "join_mode"

}


class JoinProcessor extends Processor{
  override def process(dfList: ListBuffer[DataFrame]): DataFrame = {

    val joinMethod = config.getString(JoinProcessorConfig.joinMethod,"left_outer").toLowerCase
    val joinCols = config.getStringList(JoinProcessorConfig.joinCols).asScala

    val df1 = dfList.head
    var df2 = dfList(1)
    require(joinCols.nonEmpty)

    var resultDf = spark.emptyDataFrame

    if(joinCols.length == 1){
      resultDf = df1.join(df2,joinCols,joinMethod)
    }else{
      resultDf = df1.join(df2,col(joinCols.head) === col(joinCols(1)),joinMethod)
    }

    resultDf
  }

  override def shortName(): String = "join"
}

