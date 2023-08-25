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
    val inputFieldNames=inputDF.schema.fieldNames
    val loopList= inputDF.collect().map(_.get(0))
    val taskName=config.getString("name")
    val loopSize=inputDF.count()
    val loopValuesMap=inputDF.collect().map{
      row=>
        inputFieldNames.map{
          field=>(taskName+"."+field,Seq(row.getAs[String](field)))
        }.toMap
    }.reduce{
      (map1, map2) =>
        (map1.keySet ++ map2.keySet).map { key =>
          key -> (map1.getOrElse(key, Seq.empty[String]) ++ map2.getOrElse(key, Seq.empty[String]))
        }.toMap
    }


    val tasksConfigList=config.getConfigList("tasks")
    val _var=config.getStringSafely("var")
    val _yield=config.getStringSafely("yield")
    val yieldDFs:ListBuffer[DataFrame]=ListBuffer.empty
    if(_var.nonEmpty){
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
    }else{
      for(index <- 0l until loopSize){
        tasksConfigList.map{
          config=>
            var configJson=config.root().render(ConfigRenderOptions.concise().setFormatted(true))
            loopValuesMap.keySet.foreach{
              key=>
              configJson.replaceAll("\\#\\{" + key + "\\}", loopValuesMap(key)(index.toInt))
            }
            println("loop tasks conf:")
            println(configJson)
            ConfigFactory.parseString(configJson)
        }.map(Task(_, job)).foreach(_.execute())
        if(_yield.nonEmpty){
          yieldDFs.append(job.outputMap(_yield))
        }
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
