package io.github.seabow.datax.core.pipeline.connector

import io.github.seabow.datax.common.ConfigUtils
import io.github.seabow.datax.core.pipeline.Connector
import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame

object FileConnectorConfig{
  def path = "path"
  def format ="format"
  def options="options"
  def schema="schema"
  def mode = "mode"
}

class FileConnector extends Connector with Logging{
  override def shortName(): String = "file"
  override def read(): DataFrame = {
    val path: String = ConfigUtils.getString(config, FileConnectorConfig.path)
    val format: String = ConfigUtils.getString(config, FileConnectorConfig.format)
    val options=ConfigUtils.getStringMap(config,FileConnectorConfig.options)
    val schema=ConfigUtils.getString(config,FileConnectorConfig.schema)

    /*读取数据源表*/
    var dfReader = spark.read.format(format).options(options)
    if(!schema.isEmpty)
    {
      dfReader= dfReader.schema(schema)
    }
    dfReader.load(path)
  }

  override def write(df: DataFrame): Int = {
    val format = ConfigUtils.getString(config, FileConnectorConfig.format)
    val writeMode = ConfigUtils.getString(config, FileConnectorConfig.mode, "overwrite")
    val path = ConfigUtils.getString(config, FileConnectorConfig.path).stripSuffix("/")
    val options=ConfigUtils.getStringMap(config,FileConnectorConfig.options)
    var value = 1

    df.printSchema()
    println(df.schema.toDDL)

    try {
      df.write.mode(writeMode).options(options).format(format).save(path)
    } catch {
      case t: Throwable =>
        log.error("write df failed", t)
        value = 0
    }
    value
  }

}
