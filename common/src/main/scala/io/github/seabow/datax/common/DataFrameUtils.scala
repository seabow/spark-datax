package io.github.seabow.datax.common

import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Row}

import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.AbstractIterator
import scala.concurrent.Future
import scala.util.control.Breaks.{break, breakable}

object DataFrameUtils {
  implicit class DataFrameImplicits (val df: DataFrame) {
    def repartitionByCount(countPerPartition:Long,maxPartitionNum:Int=Int.MaxValue):DataFrame ={
      val count=df.count()
      val partitionNum = Math.min( Math.ceil(count.toDouble / countPerPartition).toInt, maxPartitionNum)
      df.repartition(partitionNum)
    }

    //通过阻塞队列进行flatmap以提供更高的cpu利用率。
    def flatMapWithThreads[U : Encoder](nThreads: Int)(block: Row => TraversableOnce[U]): Dataset[U] = {
      df.mapPartitions {
        rows =>
          val ec = FutureUtils.buildExecutorContext(nThreads)
          val queue=new ArrayBlockingQueue[U](1024)
          var rowSize=0
          val executedCnt= new AtomicInteger(0)
          rows.foreach {
            row =>
              rowSize+=1
              Future {
                block(row).foreach{
                  result=>
                  queue.put(result)
                }
                executedCnt.incrementAndGet()
              }(ec)
          }
          val results= new AbstractIterator[U] {
            def hasNext: Boolean = {
              while (executedCnt.get() < rowSize) {
                if (!queue.isEmpty) {return true}
              }
              !queue.isEmpty
            }
            def next(): U = queue.take()
          }
          results
      }
    }
  }
}
