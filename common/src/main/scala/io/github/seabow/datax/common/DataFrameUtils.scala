package io.github.seabow.datax.common

import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Row}

import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.AbstractIterator
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.Success

object DataFrameUtils {
  implicit class DataFrameImplicits (val df: DataFrame) {
    def repartitionByCount(countPerPartition:Long,maxPartitionNum:Int=Int.MaxValue):DataFrame ={
      val count=df.count()
      val partitionNum = Math.min( Math.ceil(count.toDouble / countPerPartition).toInt, maxPartitionNum)
      df.repartition(partitionNum)
    }

//    def flatMapWithThreadsOld[U : Encoder](nThreads: Int)(block: Row => TraversableOnce[U]): Dataset[U] = {
//      df.mapPartitions {
//        rows =>
//          val ec = FutureUtils.buildExecutorContext(nThreads)
//          rows.map {
//            row =>
//            Await.result( Future(block(row))(ec),Duration.Inf)
//          }.flatten
//      }
//    }
    //我们需要一个性能更加好的实现，通过阻塞队列
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
            def hasNext: Boolean = executedCnt.get()<rowSize || queue.size()>0
            def next(): U = queue.take()
          }
          results
      }
    }
  }
}
