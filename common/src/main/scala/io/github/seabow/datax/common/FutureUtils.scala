package io.github.seabow.datax.common

import java.util.concurrent.Executors
import scala.concurrent.ExecutionContext

object FutureUtils {
    def buildExecutorContext(nThreads:Int):ExecutionContext ={
      val executor = Executors.newFixedThreadPool(nThreads)
      ExecutionContext.fromExecutorService(executor)
    }
}
