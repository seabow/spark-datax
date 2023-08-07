package io.github.seabow.datax.connector
import io.github.seabow.datax.common.ConfigUtils._
import io.github.seabow.datax.core.pipeline.Connector
import org.apache.spark.sql.DataFrame

object MongoConnectorConfig {
  def connection_uri = "connection_uri"
  def database ="database"
  def collection="collection"
  def aggregation_pipeline="aggregation_pipeline"
  def schema="schema"
  def mode="mode"
  // see https://www.mongodb.com/docs/spark-connector/current/configuration/

  /**
   * options:{
   * connection.uri:"mongodb://localhost:27017/"
   * database:your_database
   * collection:your_collection
   * aggregation.pipeline:"""{"$match": {"closed": false}}"""
   * }
   * @return
   */
  def options="options"
}

/**
 * The type Mongo connector.
 */
class MongoConnector extends Connector{
  override def shortName(): String = "mongodb"

  /**
   *
   *  @return DataFrame
   */
  override  def read():DataFrame={
    val connection_uri=config.getString(MongoConnectorConfig.connection_uri,"")
    val database=config.getString(MongoConnectorConfig.database,"")
    val collection=config.getString(MongoConnectorConfig.collection,"")
    val aggregation_pipeline=config.getString(MongoConnectorConfig.aggregation_pipeline,"")
    val schema=config.getString(MongoConnectorConfig.schema,"")
    val options=config.getStringMapSafely("options")
    var reader=spark.read.format("mongodb").options(
      Map("connection.uri" -> connection_uri,
        "database"->database,
        "collection" -> collection
      ))
    if(schema.nonEmpty){
      reader=reader.schema(schema)
    }
    if(aggregation_pipeline.nonEmpty){
      reader=reader.option("aggregation.pipeline",aggregation_pipeline)
    }
    reader.options(options).load()
  }

  /**
   *
   * @param df
   *  @return
   */
  override def write(df: DataFrame): Int = {
    val mode=config.getString(MongoConnectorConfig.mode,"append")
    val options=config.getStringMapSafely("options")
    df.write.format("mongodb").options(options).mode(mode).save()
    1
  }
}
