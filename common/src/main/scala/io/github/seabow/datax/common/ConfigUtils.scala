package io.github.seabow.datax.common

import com.typesafe.config._
import org.apache.commons.text.StringEscapeUtils

import scala.collection.JavaConverters._
import scala.collection.mutable

object ConfigUtils {
  /**
   * 解析自定义变量参数
   *
   * @param config
   * @param key
   * @return
   */
  def getUdvMap(udvList: List[Config]): Map[String, String] = {
    var map = Map.empty[String, String]
    udvList.map(udv => {
      val udvName = udv.getString("name")
      val udvType = udv.getString("type")
      val udvValue = udv.getString("value")

      udvType match {
        case "udf" => {
          val pattern = "(.*)\\.(.*)\\((.*)\\)".r
          val regRes = pattern.findAllIn(udvValue)
          val udfClass = regRes.group(1)
          val udfFunction = regRes.group(2)
          val udfParam = regRes.group(3)
          println(s"udf info,class:$udfClass,method:$udfFunction,param:$udfParam")
          val c = Class.forName(udfClass)
          val method = c.getDeclaredMethod(udfFunction, classOf[String])
          val finalValue = method.invoke(null, udfParam).toString
          map += (udvName -> finalValue)
        }
        case "shell"=>{
          map+=(udvName->ShellUtils.runShellForStringSubstitution(udvValue))
        }
        case _ => return map
      }
    })
    map
  }

  /**
   * 生成最终的config。
   * 1.通过$代替的来源，可以是udv，也可以是param，param的优先级高于udv。
   * @param configContent
   * @param params
   * @return
   */
  def parseAndResolveContent(configContent: String, params: Map[String, String]=Map.empty): Config = {
    var finalConfig = configContent
    // 先处理自定义的参数
    params.map { case (k, v) =>
      val findStr = "\\$\\{" + k + "\\}"
      finalConfig = finalConfig.replaceAll(findStr, StringEscapeUtils.escapeXSI(v))
    }
    val paramConfig: Config = ConfigFactory.parseMap(params.asJava)
    val resolveOptions= ConfigResolveOptions.defaults().setAllowUnresolved(true)
    val config = ConfigFactory.parseString(finalConfig).resolve(resolveOptions).resolveWith(paramConfig,resolveOptions)
    if(!config.hasPath("udv")){
      return config
    }

    // 处理UDV
    var udvParamMap:Map[String,String]=getUdvMap(config.getConfigList("udv").asScala.toList)
    val udvParamMapConfig: Config = ConfigFactory.parseMap(udvParamMap.asJava)
    println(udvParamMapConfig)
    val result= config.resolveWith(udvParamMapConfig, ConfigResolveOptions.defaults().setAllowUnresolved(true))
    finalConfig=result.root().render()
    udvParamMap.map{
      case (k,v)=>
        val findStr = "\\$\\{" + k + "\\}"
        finalConfig = finalConfig.replaceAll(findStr, v)
    }
   return ConfigFactory.parseString(finalConfig)
  }

  implicit class ImplicitConfigUtils(config: Config){
    def getString(key: String, defaultValue: String): String = {
      if (config.hasPath(key)) config.getString(key) else defaultValue
    }

    def getStringSafely(key: String): String = {
      getString( key, "")
    }

    /*获取布尔值*/
    def getBoolean( key: String, defaultValue: Boolean): Boolean = {
      if (config.hasPath(key)) config.getBoolean(key) else defaultValue
    }

    def getBooleanSafely(key: String): Boolean = {
      getBoolean(key, false)
    }

    /*获取整型数值*/
    def getInt( key: String, defaultValue: Int): Int = {
      if (config.hasPath(key)) config.getInt(key) else defaultValue
    }

    def getIntSafely(key: String): Int = {
      getInt( key, 0)
    }

    def getElement[T]( key: String): T = {
      key match {
        case key if (config.hasPath(key)) => config.getAnyRef(key).asInstanceOf[T]
      }
    }

    /*取String List*/
    def getStringListSafely(key: String): List[String] = {
      key match {
        case x if config.hasPath(x) => config.getStringList(x).asScala.toList
        case _ => Nil
      }
    }

    /*获取mutable.Map[String, String]*/
    def getStringMapSafely(key: String): mutable.Map[String, String] = {
      val map = new mutable.LinkedHashMap[String, String]
      if (config.hasPath(key)) {
        config.getConfig(key)
          .entrySet()
          .asScala
          .foreach(entry => {
            map.put(entry.getKey, entry.getValue.unwrapped().toString)
          })
      }
      map
    }

    /*获取 Map[String, List[String]]*/
    def getListMapSafely(key: String): Map[String, List[String]] = {
      var map = Map.empty[String, List[String]]
      key match {
        case x if config.hasPath(x) => {
          config.getConfig(key).
            entrySet().asScala.
            foreach(entry => {
              val key = entry.getKey
              val value = entry.getValue.atKey(key).getStringList(key).asScala.toList
              if (value.nonEmpty) {
                map += key -> entry.getValue.atKey(key).getStringList(key).asScala.toList
              }
            })
        }
        case _ => return map
      }
      map
    }
  }

}

