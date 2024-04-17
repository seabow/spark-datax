package io.github.seabow.datax.core.testutils

import io.github.seabow.datax.common.FutureUtils
import org.apache.spark.sql.{Dataset, Encoder, Row}

import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.AbstractIterator
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

object TestFutureIterator {
  def main(args: Array[String]): Unit = {
    testMapCase2
  }

  def testMapCase: Unit = {
    val it = Array(1, 2, 3).toIterator
    val ec = FutureUtils.buildExecutorContext(4)
    val result = it.map {
      ele =>
        Await.result(
          Future {
            println("start: " + ele)
            Thread.sleep(1000)
            println("end: " + ele)
            Array(ele * 1, ele * 2, ele * 3).toIterator
          }(ec), Duration.Inf)
    }.flatten
    Thread.sleep(3000)
    println("start print result")
    result.foreach(
      println(_)
    )
  }

  def testMapCase2: Unit = {
    val it = (1 to 10).toIterator
    var itSize=0
    val ec = FutureUtils.buildExecutorContext(3)
    //需要一个阻塞队列
    val queue=new ArrayBlockingQueue[Int](10)
    val executedCnt= new AtomicInteger(0)
     it.foreach {
      ele =>
        itSize+=1
        Future {
          println("start: " + ele)
          Thread.sleep(1000)
          Array(ele * 100, ele * 1000, ele * 10000).foreach(queue.put(_))
          executedCnt.incrementAndGet()
          println("end: " + ele)
        }(ec)
    }
    //TODO 需要一个迭代器
   val results= new AbstractIterator[Int] {
      def hasNext: Boolean = executedCnt.get()<itSize || queue.size()>0
      def next(): Int = queue.take()
    }
    //TODO 马上开始执行future

//    Thread.sleep(1000)
    println("start print result")
    while(results.hasNext){
      println(results.next())
    }
  }

}