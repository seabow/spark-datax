package io.github.seabow.datax.core.pipeline.processor

import io.github.seabow.datax.core.BasePipelineTest

import scala.collection.mutable.ListBuffer

class AssertProcessorTest  extends BasePipelineTest{

  test("test assert fail") {
    val df = spark.sql("select false")
    try{
    val value=new AssertProcessor().process(ListBuffer(df))
    }catch {
      case e:Throwable=>
        assert(e.isInstanceOf[AssertionError])
    }
  }

  test("test assert success") {
    val df = spark.sql("select true")
      val value=new AssertProcessor().process(ListBuffer(df))
    assert(value.isEmpty)
  }

}
