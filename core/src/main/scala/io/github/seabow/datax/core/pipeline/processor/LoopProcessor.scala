package io.github.seabow.datax.core.pipeline.processor

import com.typesafe.config.ConfigFactory
import io.github.seabow.datax.core.Task
import io.github.seabow.datax.core.pipeline.Processor
import org.apache.spark.sql.DataFrame

import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
import scala.collection.mutable.ListBuffer

/**
 * {
 *   name:loop_processor
 *   input:xxx
 *   stage:processor
 *   type:loop
 *   var:i
 *   tasks:[
 *    {
 *     stage:processor
 *     type:sql
 *     content:"select #{i} as content"
 *     }
 *   ]
 * }
 */
object LoopProcessorConfig{
  val tasks="tasks"
  val _var="var"

}

class LoopProcessor extends Processor{
  override def process(dfList: ListBuffer[DataFrame]): DataFrame = {
    val inputDF=dfList.head
    val loopList= inputDF.collect().map(_.get(0))
    val tasksConfigList=config.getConfigList("tasks")
    val _var=config.getString("var")
    for (elem <- loopList){
      tasksConfigList.map{
        config=>
         ConfigFactory.parseString( config.root().render().replaceAll("\\#\\{"+_var+"\\}",elem.toString))
      }.map(Task(_, job)).foreach(_.execute())
    }
    spark.emptyDataFrame
  }

  override def shortName(): String = "loop"
}
