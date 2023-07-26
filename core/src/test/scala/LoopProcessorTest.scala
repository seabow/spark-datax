import com.typesafe.config.ConfigFactory
import io.github.seabow.datax.core.Job
import io.github.seabow.datax.core.pipeline.processor.LoopProcessor
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.mutable.ListBuffer

class LoopProcessorTest extends  AnyFunSuite with BeforeAndAfterAll with SparkSessionTestWrapper{
  test("simple loop processor"){
    import spark.implicits._
    val dataDF = Seq(1, 2, 3).toDF("param")
    val config=
      """
        |    {
        |      name:"loop_test"
        |      type:"loop"
        |      stage:"processor"
        |      var:i
        |      tasks:[
        |        {
        |          name:"select"
        |          type:sql
        |          stage:processor
        |          content:"select #{i}"
        |        }
        |      ]
        |    }
        |""".stripMargin
    val job=Job(s"{tasks:[$config]}",spark=spark)

    val loopProcessor=new LoopProcessor
    loopProcessor.job(job)
    loopProcessor.config(ConfigFactory.parseString(config))
    loopProcessor.process(ListBuffer(dataDF))
  }
}
