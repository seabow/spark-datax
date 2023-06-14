package org.apache.spark.datax.utils

import io.github.seabow.datax.core.TaskRegister
import io.github.seabow.datax.core.pipeline.{Connector, Processor}
import org.apache.spark.util.Utils

import java.util.ServiceLoader
import scala.collection.JavaConverters._
object ClassLoaderUtils {

  /**
   * Class loader
   * @param shortName
   * @return
   */
  def lookUp(shortName:String):Class[_]=
  {
    val loader = Utils.getContextOrSparkClassLoader
    val serviceLoader = ServiceLoader.load(classOf[TaskRegister], loader)
    serviceLoader.asScala.filter(_.shortName().equals(shortName)) match {
      case head::Nil=>
        head.getClass
      case _ => throw new Exception(s"Could not find $shortName in ${serviceLoader}")
    }
  }

  def getPipelineInstance(shortName:String):Any={
    lookUp(shortName) match {
      case connector if classOf[Connector].isAssignableFrom(connector)=>
        connector.newInstance().asInstanceOf[Connector]
      case processor if classOf[Processor].isAssignableFrom(processor)=>
        processor.newInstance().asInstanceOf[Processor]
      case _ => throw new Exception(s"Could not find $shortName")
    }
  }

}
