package io.github.seabow.datax.core.pipeline.processor


import io.github.seabow.datax.common.SparkUtils
import io.github.seabow.datax.core.pipeline.Processor
import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame

import scala.collection.mutable.ListBuffer

object SqlProcessorConfig {
  def mode = "mode"
  def content = "content"
}

object SqlProcessor {

}

class SqlProcessor extends Processor with Logging{
  override def process(dfList: ListBuffer[DataFrame]): DataFrame = {
    val mode=config.getString(SqlProcessorConfig.mode)
    val content=config.getString(SqlProcessorConfig.content)
    var finalDF = spark.emptyDataFrame
    //将输入的reader或processor按其name映射为tempView
    if(dfList.nonEmpty){
      val tableNamesArray=config.getString("input").split(",")
      if(dfList.size==tableNamesArray.size){
        dfList.indices.foreach{
          i=>
            val viewName = tableNamesArray(i)
            dfList(i).createOrReplaceTempView(viewName)
            log.info(s"view name:$viewName")
        }
      }else{
        throw new Exception("input count mismatch size of dfList")
      }
    }

    val sql =mode match {
      case "sql"=> content
      /*支持传入多个sql文件，使用逗号分隔，输出的字符串将多个文件的内容合并*/
      case "file"=>{
        content.split(",").map(file=>{
          SparkUtils.getFileContent(file)
        }).mkString

      }
      case _=>throw new Exception("mode字段仅支持sql或者file。")
    }

    log.warn(s"sql内容：\n${sql}")

    /*去除注释及windows换行符*/
    val pureSql=sql.split("\n").map(_.trim)
      .map(_.replace("\r",""))
      .filterNot(_.startsWith("--")).mkString(" ")

    log.warn(s"pureSql内容：\n${pureSql}")

    /*根据 ; 分隔sql脚本，逐条执行,返回最后一个sql语句的结果*/
    val sqlArr=pureSql.split(";")
    sqlArr.length match {
      case 0=>throw new IllegalArgumentException ("sql statement is null")
      case 1=>finalDF=spark.sql(sqlArr.last)
      case _=>{
        sqlArr.init.map(spark.sql(_))
        finalDF=spark.sql(sqlArr.last)
      }
    }

    finalDF.printSchema()
    finalDF
  }

  override def shortName(): String = "sql"
}

