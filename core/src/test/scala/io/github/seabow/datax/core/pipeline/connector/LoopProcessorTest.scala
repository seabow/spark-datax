package io.github.seabow.datax.core.pipeline.connector

import com.typesafe.config.ConfigFactory
import io.github.seabow.datax.core.pipeline.processor.LoopProcessor
import io.github.seabow.datax.core.{BasePipelineTest, Job}

import scala.collection.mutable.ListBuffer

class LoopProcessorTest extends BasePipelineTest{
  test("simple loop processor use var"){
    import spark.implicits._
    val dataDF = Seq(1, 2, 3).toDF("param")
    val config=
      """
        |    {
        |      name:"loop_test"
        |      type:"loop"
        |      stage:"processor"
        |      input:NULL
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

  test("simple empty df use var"){
    val dataDF = spark.emptyDataFrame
    val config=
      """
        |    {
        |      name:"loop_test"
        |      type:"loop"
        |      input:NULL
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

  test("simple loop processor use input params"){
    val config=
      """
        |
        |  {
        |    name:"loopParams"
        |    type:sql
        |    stage:processor
        |    content:"select explode(array(1,2,3)) as param"
        |    mode:sql
        |  }
        | {
        |      name:"loop_test"
        |      type:"loop"
        |      input:loopParams
        |      stage:"processor"
        |      tasks:[
        |        {
        |          name:"select"
        |          type:sql
        |          stage:processor
        |          content:"select #{loopParams.param}"
        |        }
        |      ]
        |  }
        |
        |""".stripMargin
    val job=Job(s"{tasks:[$config]}",spark=spark)
    job.execute()
    job.outputMap("select_1").show()
  }
}
