package io.github.seabow.datax.core.pipeline.processor

import com.typesafe.config.{ConfigFactory, ConfigRenderOptions}
import io.github.seabow.datax.common.ConfigUtils.ImplicitConfigUtils
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
  val _yield="yield"
}

class LoopProcessor extends Processor{
  override def process(dfList: ListBuffer[DataFrame]): DataFrame = {
    val inputDF=dfList.head
    val loopList= inputDF.collect().map(_.get(0))
    val tasksConfigList=config.getConfigList("tasks")
    val _var=config.getString("var")
    val _yield=config.getStringSafely("yield")
    val yieldDFs:ListBuffer[DataFrame]=ListBuffer.empty
    for (elem <- loopList){
      tasksConfigList.map{
        config=>
         val configInstanceStr = config.root().render(ConfigRenderOptions.concise().setFormatted(true)).replaceAll("\\#\\{" + _var + "\\}", elem.toString)
          println("loop tasks conf:")
          println(configInstanceStr)
          ConfigFactory.parseString( configInstanceStr)
      }.map(Task(_, job)).foreach(_.execute())
      if(_yield.nonEmpty){
        yieldDFs.append(job.outputMap(_yield))
      }
    }
    if(yieldDFs.nonEmpty){
      yieldDFs.reduce((a,b)=>a.unionByName(b,true))
    }else{
      spark.emptyDataFrame
    }

  }

  override def shortName(): String = "loop"
}
