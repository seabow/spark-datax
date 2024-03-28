package org.apache.spark.sql.utils

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser.parseMultipartIdentifier
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.IdentifierHelper
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.execution.datasources.v2.V2SessionCatalog
import org.apache.spark.sql.internal.HiveSerDe

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

  private def getTableMetadata(identifier: Identifier, catalog: CatalogPlugin): CatalogTable = {
    catalog match {
      case _: V2SessionCatalog =>
        SparkSession.active.sessionState.catalog.getTableMetadata(identifier.asTableIdentifier)
      case _: CatalogExtension =>
        SparkSession.active.sessionState.catalog.getTableMetadata(identifier.asTableIdentifier)
      case other => throw new IllegalStateException(s"cannot find table metadata :$other")
    }
  }

  def getTableMetadata(table:String):CatalogTable = {
    val (catalogPlugin,identifer)=getCatalogAndIdentifier(table).getOrElse(throw new IllegalStateException("can not find catalog"))
    convertTableMetadata(getTableMetadata(identifer,catalogPlugin))
  }

  def tableExists(table:String):Boolean = {
    val (catalogPlugin,identifer)=getCatalogAndIdentifier(table).getOrElse(throw new IllegalStateException("can not find catalog"))
    tableExists(identifer,catalogPlugin)
  }
  private def convertTableMetadata(tableMetadata: CatalogTable): CatalogTable = {
    val hiveSerde = HiveSerDe(
      serde = tableMetadata.storage.serde,
      inputFormat = tableMetadata.storage.inputFormat,
      outputFormat = tableMetadata.storage.outputFormat)
    // Looking for Spark data source that maps to to the Hive serde.
    // TODO: some Hive fileformat + row serde might be mapped to Spark data source, e.g. CSV.
    val source = HiveSerDe.serdeToSource(hiveSerde)
    if (source.isEmpty) {
      val builder = new StringBuilder
      hiveSerde.serde.foreach { serde =>
        builder ++= s" SERDE: $serde"
      }
      hiveSerde.inputFormat.foreach { format =>
        builder ++= s" INPUTFORMAT: $format"
      }
      hiveSerde.outputFormat.foreach { format =>
        builder ++= s" OUTPUTFORMAT: $format"
      }
      throw new IllegalStateException("can't map hive to spark"+builder.toString())
    } else {
      // TODO: should we keep Hive serde properties?
      val newStorage = tableMetadata.storage.copy(properties = Map.empty)
      tableMetadata.copy(provider = source, storage = newStorage)
    }
  }

  lazy override protected val catalogManager: CatalogManager = SparkSession.active.sessionState.catalogManager
}
