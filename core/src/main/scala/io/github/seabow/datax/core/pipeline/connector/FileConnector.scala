package io.github.seabow.datax.core.pipeline.connector
import io.github.seabow.datax.common.ConfigUtils.ImplicitConfigUtils
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
    val path: String =config.getStringSafely(FileConnectorConfig.path)
    val format: String =config.getStringSafely(FileConnectorConfig.format)
    val options=config.getStringMapSafely(FileConnectorConfig.options)
    val schema=config.getStringSafely(FileConnectorConfig.schema)

    /*读取数据源表*/
    var dfReader = spark.read.options(options)
    if(!schema.isEmpty)
    {
      dfReader= dfReader.schema(schema)
    }
    if(format.nonEmpty){
      dfReader=dfReader.format(format)
    }
    dfReader.load(path)
  }

  override def write(df: DataFrame): Int = {
    val format = config.getStringSafely( FileConnectorConfig.format)
    val writeMode = config.getString( FileConnectorConfig.mode, "overwrite")
    val path = config.getStringSafely( FileConnectorConfig.path).stripSuffix("/")
    val options=config.getStringMapSafely(FileConnectorConfig.options)
    var value = 1

    df.printSchema()
    println(df.schema.toDDL)
    var dfWriter=df.write.mode(writeMode).options(options)
    if(format.nonEmpty){
      dfWriter=dfWriter.format(format)
    }

    try {
      dfWriter.save(path)
    } catch {
      case t: Throwable =>
        log.error("write df failed", t)
        value = 0
    }
    value
  }

}
