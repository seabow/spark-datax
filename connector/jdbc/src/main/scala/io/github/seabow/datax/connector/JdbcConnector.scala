package io.github.seabow.datax.connector
import io.github.seabow.datax.common.ConfigUtils._
import io.github.seabow.datax.core.pipeline.Connector
import org.apache.spark.sql.DataFrame

object JdbcConnectorConfig {
  def schema="schema"
  def mode="mode"
  def db_type="db_type"
  def options="options"
}

/**
 * The type Jdbc connector.
 */
class JdbcConnector extends Connector{
  val dbTypeDriverMap=Map(
    "mysql"->"com.mysql.jdbc.Driver"
  )


  override def shortName(): String = "jdbc"

  /**
   *
   *  @return DataFrame
   */
  override  def read():DataFrame={
    val schema=config.getString(JdbcConnectorConfig.schema,"")
    val options=config.getStringMapSafely("options")
    val db_type=config.getString(JdbcConnectorConfig.db_type,"mysql")
    var reader=spark.read.format("jdbc").option("driver",dbTypeDriverMap(db_type))
    if(schema.nonEmpty){
      reader=reader.schema(schema)
    }
    reader.options(options).load()
  }

  /**
   *
   * @param df
   *  @return
   */
  override def write(df: DataFrame): Int = {
    val mode=config.getString(JdbcConnectorConfig.mode,"append")
    val options=config.getStringMapSafely("options")
    df.write.format("jdbc").options(options).mode(mode).save()
    1
  }
}
