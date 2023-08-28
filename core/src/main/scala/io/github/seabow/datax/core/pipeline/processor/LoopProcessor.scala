package io.github.seabow.datax.core.pipeline.processor

import com.typesafe.config.{ConfigFactory, ConfigRenderOptions}
import io.github.seabow.datax.common.ConfigUtils.ImplicitConfigUtils
import io.github.seabow.datax.core.Task
import io.github.seabow.datax.core.pipeline.Processor
import org.apache.spark.sql.DataFrame
import org.codehaus.jackson.map.ObjectMapper
import org.codehaus.jackson.node.ObjectNode

import java.util.concurrent.Executors
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._


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
  val par="par"
}

class LoopProcessor extends Processor{
  override def process(dfList: ListBuffer[DataFrame]): DataFrame = {
    val inputDF=dfList.head
    val inputFieldNames=inputDF.schema.fieldNames
    val loopList= inputDF.collect().map(_.get(0))
    val taskName=config.getString("input")
    val par=config.getIntSafely(LoopProcessorConfig.par)
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
      assert(_yield.isEmpty)
      val threadPoolSize=if(par>0){par}else{loopSize.toInt}
      val executor: ExecutionContextExecutor = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(threadPoolSize))
      val loopTasks= (0l until loopSize).map{
       index=>
         val localTaskNames=tasksConfigList.map(_.getString("name")).toSet
        val tasks=tasksConfigList.map{
          config=>
            var configJson=config.root().render(ConfigRenderOptions.concise().setFormatted(true))
            val objectMapper=new ObjectMapper()
            val jsonNode=objectMapper.readTree(configJson).asInstanceOf[ObjectNode]
            jsonNode.put("name",jsonNode.get("name").getTextValue+"_"+index)
            if(jsonNode.has("input") && localTaskNames.contains( jsonNode.get("input").getTextValue))
            {
                jsonNode.put("input",jsonNode.get("input").getTextValue+"_"+index)
            }
            configJson=jsonNode.toString
            loopValuesMap.keySet.foreach{
              key=>
                configJson= configJson.replaceAll("\\#\\{" + key + "\\}", loopValuesMap(key)(index.toInt))
            }
            println(s"loop task conf [$index]:")
           val configRendered= ConfigFactory.parseString(configJson)
            println(configRendered.root().render(ConfigRenderOptions.concise().setFormatted(true)))
            configRendered
        }.map(Task(_, job))
        Future{tasks.foreach(_.execute())}(executor)
      }
      Await.result(Future.sequence(loopTasks), scala.concurrent.duration.Duration.Inf)
    }

    if(yieldDFs.nonEmpty){
      yieldDFs.reduce((a,b)=>a.unionByName(b,true))
    }else{
      spark.emptyDataFrame
    }

  }

  override def shortName(): String = "loop"
}
