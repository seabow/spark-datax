package io.github.seabow.datax.core

import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession

trait ContextLoader {
  var config:Config=_
  lazy val spark=SparkSession.getActiveSession.get
  def init(config:Config):Unit={
    this.config=config
  }
}
