package io.github.seabow.datax.core.pipeline

import io.github.seabow.datax.core.{ContextLoader, TaskRegister}
import org.apache.spark.sql.DataFrame

trait Connector extends TaskRegister with ContextLoader{
  def read():DataFrame={
    throw new UnsupportedOperationException("Not implemented")
  }
  def write( df: DataFrame):Int={
    throw new UnsupportedOperationException("Not implemented")
  }
}
