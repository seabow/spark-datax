package io.github.seabow.datax.core.pipeline

import io.github.seabow.TaskCleaner
import io.github.seabow.datax.core.{ContextLoader, TaskRegister}
import org.apache.spark.sql.DataFrame

trait Connector extends TaskRegister with ContextLoader with TaskCleaner{
  /**
   *
   * @return DataFrame
   */
  def read():DataFrame={
    throw new UnsupportedOperationException("Not implemented")
  }

  /**
   *
   * @param df
   * @return
   */
  def write( df: DataFrame):Int={
    throw new UnsupportedOperationException("Not implemented")
  }
}
