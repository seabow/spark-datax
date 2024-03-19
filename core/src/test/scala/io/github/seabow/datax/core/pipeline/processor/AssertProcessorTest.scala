package io.github.seabow.datax.core.pipeline.processor

import io.github.seabow.datax.core.SparkSessionTestWrapper
import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.mutable.ListBuffer

class AssertProcessorTest extends AnyFunSuite with SparkSessionTestWrapper{

  test("test assert fail") {
    println(spark.sqlContext.getConf(CATALOG_IMPLEMENTATION.key))

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
