package io.github.seabow.datax.common

import java.util.concurrent.Executors
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.concurrent.duration.DurationLong


object FutureUtils {
    def buildExecutorContext(nThreads:Int):ExecutionContext ={
      val executor = Executors.newFixedThreadPool(nThreads)
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
