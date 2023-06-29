package io.github.seabow.datax.core

import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession

trait ContextLoader {
  var config:Config=_
  var job:Job=_
  lazy val spark=SparkSession.getActiveSession.get
  def config(config:Config):ContextLoader={
    this.config=config
    this
  }
  def job(job:Job):ContextLoader={
    this.job=job
    this
  }
}
