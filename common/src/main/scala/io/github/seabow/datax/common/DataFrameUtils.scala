package io.github.seabow.datax.common

import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Row}

import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.AbstractIterator
import scala.collection.mutable.ListBuffer
import scala.concurrent.Future
import scala.util.{Failure, Success}
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
          val exceptions:ListBuffer[Throwable] = ListBuffer.empty[Throwable]
          def addException(element: Throwable): Unit = exceptions.synchronized {
            exceptions.append(element)
          }
          rows.foreach {
            row =>
              rowSize+=1
              Future {
                  block(row).foreach{
                    result=>
                      queue.put(result)
                  }
              }(ec).onComplete{
                case Success(response) => executedCnt.incrementAndGet()
                case Failure(e) => addException(e)
              }(ec)
          }
          val results= new AbstractIterator[U] {
            def hasNext: Boolean = this.synchronized{
              while (executedCnt.get() < rowSize) {
                if (!queue.isEmpty) {return true}
                if(exceptions.nonEmpty){exceptions.foreach(e=>throw e)}
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
