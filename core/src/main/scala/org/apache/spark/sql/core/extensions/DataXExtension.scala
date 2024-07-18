package org.apache.spark.sql.core.extensions

import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo}
import org.apache.spark.sql.core.udfs.{Assert, ListPath, PathExists}

class DataXExtension extends (SparkSessionExtensions=>Unit){
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectFunction(
      FunctionIdentifier("list_path"),
      new ExpressionInfo(
        classOf[ListPath].getName, "list_path"),
      (expressions: Seq[Expression]) => {
        ListPath(expressions(0))
      }
    )
    extensions.injectFunction(
      FunctionIdentifier("path_exists"),
      new ExpressionInfo(
        classOf[PathExists].getName, "path_exists"),
      (expressions: Seq[Expression]) => {
        PathExists(expressions(0))
      }
    )

    extensions.injectFunction(
      FunctionIdentifier("assert"),
      new ExpressionInfo(
        classOf[Assert].getName, "assert"),
      (expressions: Seq[Expression]) => {
        Assert(expressions(0))
      }
    )
  }
}
