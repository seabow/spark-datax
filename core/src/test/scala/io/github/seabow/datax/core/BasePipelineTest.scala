package io.github.seabow.datax.core

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import io.github.seabow.datax.core.mock.MockData
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class BasePipelineTest   extends  AnyFunSuite with BeforeAndAfterAll with DatasetComparer with SparkSessionTestWrapper{
  val mockDF=MockData.mockDF
}
