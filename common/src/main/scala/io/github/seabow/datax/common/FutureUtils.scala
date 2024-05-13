package io.github.seabow.datax.common

import org.apache.spark.internal.Logging

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{Executors, ThreadFactory}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.concurrent.duration.DurationLong


object FutureUtils extends Logging{
    def buildExecutorContext(nThreads:Int,isDaemon:Boolean=false):ExecutionContext ={
      val executor = Executors.newFixedThreadPool(nThreads,  new ThreadFactory() {
        final private val counter = new AtomicInteger(0)
        override def newThread(r: Runnable): Thread = {
          val thread = new Thread(r, "datax-thread-" + counter.getAndIncrement)
          thread.setUncaughtExceptionHandler((t: Thread, e: Throwable) => logError(s"Uncaught thread error of thread: ${ t.getName}", e))
          thread.setDaemon(isDaemon)
          thread
        }
      })
      ExecutionContext.fromExecutorService(executor)
    }

    def runWithTimeout[T]
    (timeout: Long,throwTimeoutException:Boolean=true)
    (f: => T)
    (implicit ec: ExecutionContext=global)
    : Option[T] = {
      try {
        Some(Await.result(Future(f)(ec), timeout.seconds))
      } catch {
        case e: TimeoutException =>
          if(throwTimeoutException) {throw e} else None
      }
    }

     def retry[T](times: Int)(block: => T): T = {
      try {
        block
      } catch {
        case e: Exception if times > 0 => retry(times - 1)(block)
        case e: Exception => throw e
      }
     }

}
