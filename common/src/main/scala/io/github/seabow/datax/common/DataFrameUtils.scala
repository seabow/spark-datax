package io.github.seabow.datax.common

import org.apache.spark.sql.DataFrame

object DataFrameUtils {
  implicit class DataFrameImplicits (val df: DataFrame) {
    def repartitionByCount(countPerPartition:Long,maxPartitionNum:Int=Int.MaxValue):DataFrame ={
      val count=df.count()
      val partitionNum = Math.min( Math.ceil(count.toDouble / countPerPartition).toInt, maxPartitionNum)
      df.repartition(partitionNum)
    }
  }
}
