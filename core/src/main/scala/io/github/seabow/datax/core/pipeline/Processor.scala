package io.github.seabow.datax.core.pipeline

import io.github.seabow.TaskCleaner
import io.github.seabow.datax.core.{ContextLoader, TaskRegister}
import org.apache.spark.sql.DataFrame

import scala.collection.mutable.ListBuffer

trait Processor  extends TaskRegister with ContextLoader  with TaskCleaner{
   def process(dfList: ListBuffer[DataFrame]):DataFrame
}
