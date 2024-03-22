package org.apache.spark.sql.utils

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser.parseMultipartIdentifier
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.execution.datasources.v2.V2SessionCatalog

object SparkCatalogUtils extends LookupCatalog{

 private def getCatalogAndIdentifier(table:String):Option[(CatalogPlugin,Identifier)]={
    parseMultipartIdentifier(table) match {
      case CatalogAndIdentifier(catalog, ident) =>
       Some(catalog, ident)
      case _=>None
    }
  }

 private def tableExists(identifier:Identifier,catalog:CatalogPlugin):Boolean = {
    catalog match {
      case v2SessionCatalog:V2SessionCatalog =>
        v2SessionCatalog.tableExists(identifier)
      case tableCatalog:TableCatalog =>
        tableCatalog.tableExists(identifier)
      case _=>throw new IllegalStateException("cannot find table exists")
    }
  }

  def tableExists(table:String):Boolean = {
    val (catalogPlugin,identifer)=getCatalogAndIdentifier(table).getOrElse(throw new IllegalStateException("can not find catalog"))
    tableExists(identifer,catalogPlugin)
  }


  lazy override protected val catalogManager: CatalogManager = SparkSession.active.sessionState.catalogManager
}
