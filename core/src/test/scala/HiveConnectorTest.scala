import com.typesafe.config.ConfigFactory
import io.github.seabow.datax.core.pipeline.connector.HiveConnector
import org.apache.spark.sql.functions.{col, struct}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class HiveConnectorTest  extends  AnyFunSuite with BeforeAndAfterAll with SparkSessionTestWrapper {

  override def beforeAll(){
    //create a test hive table
    spark.sql(
      """
        |create table if not exists test_hive_table(
        | name string,
        | age int,
        | info string
        |) stored as orc
        |""".stripMargin)
  }

  test("fill null to unresolved fields"){
    val data = Seq(("Value 1", 10), ("Value 2", 20), ("Value 3", 30))
    val df = spark.createDataFrame(data).toDF("name", "age")
    val config=
      """
        |    {
        |      name:"write"
        |      type:"hive"
        |      stage:"writer"
        |      mode:"overwrite"
        |      table:"test_hive_table"
        |    }
        |""".stripMargin
    val hiveConnector=new HiveConnector
    hiveConnector.config(ConfigFactory.parseString(config))
    hiveConnector.write(df)
  }

  test("auto cast"){
    val data = Seq((1, "10"), (2, "20"), (3, "30"))
    val df = spark.createDataFrame(data).toDF("name", "age").withColumn("info",struct(col("name"),col("age")))
    val config=
      """
        |    {
        |      name:"write"
        |      type:"hive"
        |      stage:"writer"
        |      mode:"overwrite"
        |      table:"test_hive_table"
        |    }
        |""".stripMargin
    val hiveConnector=new HiveConnector
    hiveConnector.config(ConfigFactory.parseString(config))
    hiveConnector.write(df)
  }

  override def   afterAll(){
   spark.sql("drop table if exists test_hive_table")
  }
}
