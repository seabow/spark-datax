package org.apache.spark.sql.errors

import org.apache.spark.SparkException
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.util.FailFastMode
import org.apache.spark.sql.errors.QueryParsingErrors.{toSQLId, toSQLType}
import org.apache.spark.sql.types.DataType

object Implicits {
  implicit class QueryComiplationErrorsImplicit(queryCompilationErrors: QueryErrorsBase){
    def unknownProtobufMessageTypeError(
                                         descriptorName: String,
                                         containingType: String): Throwable = {
      new AnalysisException(
        errorClass = "UNKNOWN_PROTOBUF_MESSAGE_TYPE",
        messageParameters = Array(
          "descriptorName:" + descriptorName,
          "containingType:"+ containingType))
    }

    def cannotFindCatalystTypeInProtobufSchemaError(catalystFieldPath: String): Throwable = {
      new AnalysisException(
        errorClass = "NO_SQL_TYPE_IN_PROTOBUF_SCHEMA",
        messageParameters = Array("catalystFieldPath:" + catalystFieldPath))
    }


    def cannotFindProtobufFieldInCatalystError(field: String): Throwable = {
      new AnalysisException(
        errorClass = "PROTOBUF_FIELD_MISSING_IN_SQL_SCHEMA",
        messageParameters = Array("field:" + field))
    }

    def protobufFieldMatchError(field: String,
                                protobufSchema: String,
                                matchSize: String,
                                matches: String): Throwable = {
      new AnalysisException(
        errorClass = "PROTOBUF_FIELD_MISSING",
        messageParameters = Array(
          "field:" + field,
          "protobufSchema:" + protobufSchema,
          "matchSize:" + matchSize,
          "matches:" + matches))
    }

    def unableToLocateProtobufMessageError(messageName: String): Throwable = {
      new AnalysisException(
        errorClass = "PROTOBUF_MESSAGE_NOT_FOUND",
        messageParameters = Array("messageName:" + messageName))
    }

    def descriptorParseError(descFilePath: String, cause: Throwable): Throwable = {
      new AnalysisException(
        errorClass = "CANNOT_PARSE_PROTOBUF_DESCRIPTOR",
        messageParameters = Array("descFilePath:" + descFilePath),
        cause = Option(cause))
    }

    def cannotFindDescriptorFileError(filePath: String, cause: Throwable): Throwable = {
      new AnalysisException(
        errorClass = "PROTOBUF_DESCRIPTOR_FILE_NOT_FOUND",
        messageParameters = Array("filePath:" + filePath),
        cause = Option(cause))
    }

    def foundRecursionInProtobufSchema(fieldDescriptor: String): Throwable = {
      new AnalysisException(
        errorClass = "RECURSIVE_PROTOBUF_SCHEMA",
        messageParameters = Array("fieldDescriptor:" + fieldDescriptor))
    }

    def protobufFieldTypeMismatchError(field: String): Throwable = {
      new AnalysisException(
        errorClass = "PROTOBUF_FIELD_TYPE_MISMATCH",
        messageParameters = Array("field:" + field))
    }

    def protobufClassLoadError(
                                protobufClassName: String,
                                explanation: String,
                                cause: Throwable = null): Throwable = {
      new AnalysisException(
        errorClass = "CANNOT_LOAD_PROTOBUF_CLASS",
        messageParameters = Array(
          "protobufClassName:" + protobufClassName,
          "explanation:" + explanation
        ),
        cause = Option(cause))
    }

    def protobufDescriptorDependencyError(dependencyName: String): Throwable = {
      new AnalysisException(
        errorClass = "PROTOBUF_DEPENDENCY_NOT_FOUND",
        messageParameters = Array("dependencyName:" + dependencyName))
    }

    def failedParsingDescriptorError(descFilePath: String, cause: Throwable): Throwable = {
      new AnalysisException(
        errorClass = "CANNOT_CONSTRUCT_PROTOBUF_DESCRIPTOR",
        messageParameters = Array("descFilePath:" + descFilePath),
        cause = Option(cause))
    }

    def protobufTypeUnsupportedYetError(protobufType: String): Throwable = {
      new AnalysisException(
        errorClass = "PROTOBUF_TYPE_NOT_SUPPORT",
        messageParameters = Array("protobufType:" + protobufType))
    }

    def malformedProtobufMessageDetectedInMessageParsingError(e: Throwable): Throwable = {
      new SparkException(
        errorClass = "MALFORMED_PROTOBUF_MESSAGE",
        messageParameters = Array(
          "failFastMode:" + FailFastMode.name),
        cause = e)
    }

    def cannotConvertProtobufTypeToCatalystTypeError(
                                                      protobufType: String,
                                                      sqlType: DataType,
                                                      cause: Throwable): Throwable = {
      new AnalysisException(
        errorClass = "CANNOT_CONVERT_PROTOBUF_MESSAGE_TYPE_TO_SQL_TYPE",
        messageParameters = Array(
          "protobufType:" + protobufType,
          "toType:" + toSQLType(sqlType)),
        cause = Option(cause))
    }

    def notNullConstraintViolationArrayElementError(path: Seq[String]): Throwable = {
      new AnalysisException(
        errorClass = "NOT_NULL_CONSTRAINT_VIOLATION.ARRAY_ELEMENT",
        messageParameters = Array("columnPath:" + toSQLId(path)))
    }

    def notNullConstraintViolationMapValueError(path: Seq[String]): Throwable = {
      new AnalysisException(
        errorClass = "NOT_NULL_CONSTRAINT_VIOLATION.MAP_VALUE",
        messageParameters = Array("columnPath:" + toSQLId(path)))
    }

    def invalidByteStringFormatError(unsupported: Any): Throwable = {
      new AnalysisException(
        errorClass = "INVALID_BYTE_STRING",
        messageParameters = Array(
          "unsupported:" + unsupported.toString,
          "class:" + unsupported.getClass.toString))
    }

    def cannotConvertProtobufTypeToSqlTypeError(
                                                 protobufColumn: String,
                                                 sqlColumn: Seq[String],
                                                 protobufType: String,
                                                 sqlType: DataType): Throwable = {
      new AnalysisException(
        errorClass = "CANNOT_CONVERT_PROTOBUF_FIELD_TYPE_TO_SQL_TYPE",
        messageParameters = Array(
          "protobufColumn:" + protobufColumn,
          "sqlColumn:" + toSQLId(sqlColumn),
          "protobufType:" + protobufType,
          "sqlType:" + toSQLType(sqlType)))
    }

    def cannotConvertSqlTypeToProtobufError(
                                             protobufType: String,
                                             sqlType: DataType,
                                             cause: Throwable): Throwable = {
      new AnalysisException(
        errorClass = "UNABLE_TO_CONVERT_TO_PROTOBUF_MESSAGE_TYPE",
        messageParameters = Array(
          "protobufType:" + protobufType,
          "toType:" + toSQLType(sqlType)),
        cause = Option(cause))
    }

    def cannotConvertCatalystTypeToProtobufEnumTypeError(
                                                          sqlColumn: Seq[String],
                                                          protobufColumn: String,
                                                          data: String,
                                                          enumString: String): Throwable = {
      new AnalysisException(
        errorClass = "CANNOT_CONVERT_SQL_TYPE_TO_PROTOBUF_ENUM_TYPE",
        messageParameters = Array(
          "sqlColumn:" + toSQLId(sqlColumn),
          "protobufColumn:" + protobufColumn,
          "data:" + data,
          "enumString:" + enumString))
    }

    def cannotConvertCatalystTypeToProtobufTypeError(
                                                      sqlColumn: Seq[String],
                                                      protobufColumn: String,
                                                      sqlType: DataType,
                                                      protobufType: String): Throwable = {
      new AnalysisException(
        errorClass = "CANNOT_CONVERT_SQL_TYPE_TO_PROTOBUF_FIELD_TYPE",
        messageParameters = Array(
          "sqlColumn:" + toSQLId(sqlColumn),
          "protobufColumn:" + protobufColumn,
          "sqlType:" + toSQLType(sqlType),
          "protobufType:" + protobufType))
    }

  }

}
