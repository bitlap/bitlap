/* Copyright (c) 2023 bitlap.org */
package org.bitlap.jdbc

import java.sql.{ Array as _, * }

import org.bitlap.client.BitlapClient
import org.bitlap.common.BitlapConf
import org.bitlap.common.utils.JsonEx
import org.bitlap.network.enumeration.{ GetInfoType, TypeId }
import org.bitlap.network.handles.SessionHandle
import org.bitlap.network.serde.BitlapSerde

/** bitlap 数据库元数据
 *
 *  @author
 *    梦境迷离
 *  @since 2021/6/12
 *  @version 1.0
 */
class BitlapDatabaseMetaData(
  private val connection: Connection,
  private val session: SessionHandle,
  private val client: BitlapClient)
    extends DatabaseMetaData
    with BitlapSerde {
  override def allProceduresAreCallable(): Boolean = throw new SQLFeatureNotSupportedException("Method not supported")

  override def allTablesAreSelectable(): Boolean = throw new SQLFeatureNotSupportedException("Method not supported")

  override def getURL: String = throw new SQLFeatureNotSupportedException("Method not supported")

  override def getUserName: String = throw new SQLFeatureNotSupportedException("Method not supported")

  override def isReadOnly: Boolean = throw new SQLFeatureNotSupportedException("Method not supported")

  override def nullsAreSortedHigh(): Boolean = throw new SQLFeatureNotSupportedException("Method not supported")

  override def nullsAreSortedLow(): Boolean = throw new SQLFeatureNotSupportedException("Method not supported")

  override def nullsAreSortedAtStart(): Boolean = throw new SQLFeatureNotSupportedException("Method not supported")

  override def nullsAreSortedAtEnd(): Boolean = throw new SQLFeatureNotSupportedException("Method not supported")

  override def getDatabaseProductName: String =
    val result = client.getInfo(session, GetInfoType.DbmsName).value
    deserialize[String](TypeId.StringType, result)

  override def getDatabaseProductVersion: String =
    val result = client.getInfo(session, GetInfoType.DbmsVer).value
    deserialize[String](TypeId.StringType, result)

  def getDatabaseConf: BitlapConf = {
    val result = client.getInfo(session, GetInfoType.ServerConf).value
    new BitlapConf(JsonEx.jsonAsMap(deserialize[String](TypeId.StringType, result)))
  }

  override def getDriverName: String = Constants.NAME

  override def getDriverVersion: String = "0"

  override def getDriverMajorVersion: Int = Constants.MAJOR_VERSION

  override def getDriverMinorVersion: Int = Constants.MINOR_VERSION

  override def usesLocalFiles(): Boolean = throw new SQLFeatureNotSupportedException("Method not supported")

  override def usesLocalFilePerTable(): Boolean = throw new SQLFeatureNotSupportedException("Method not supported")

  override def supportsMixedCaseIdentifiers(): Boolean = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def storesUpperCaseIdentifiers(): Boolean = throw new SQLFeatureNotSupportedException("Method not supported")

  override def storesLowerCaseIdentifiers(): Boolean = throw new SQLFeatureNotSupportedException("Method not supported")

  override def storesMixedCaseIdentifiers(): Boolean = throw new SQLFeatureNotSupportedException("Method not supported")

  override def supportsMixedCaseQuotedIdentifiers(): Boolean = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def storesUpperCaseQuotedIdentifiers(): Boolean = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def storesLowerCaseQuotedIdentifiers(): Boolean = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def storesMixedCaseQuotedIdentifiers(): Boolean = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def getIdentifierQuoteString: String = throw new SQLFeatureNotSupportedException("Method not supported")

  override def getSQLKeywords: String = throw new SQLFeatureNotSupportedException("Method not supported")

  override def getNumericFunctions: String = throw new SQLFeatureNotSupportedException("Method not supported")

  override def getStringFunctions: String = throw new SQLFeatureNotSupportedException("Method not supported")

  override def getSystemFunctions: String = throw new SQLFeatureNotSupportedException("Method not supported")

  override def getTimeDateFunctions: String = throw new SQLFeatureNotSupportedException("Method not supported")

  override def getSearchStringEscape: String = throw new SQLFeatureNotSupportedException("Method not supported")

  override def getExtraNameCharacters: String = ""

  override def supportsAlterTableWithAddColumn(): Boolean = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def supportsAlterTableWithDropColumn(): Boolean = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def supportsColumnAliasing(): Boolean = throw new SQLFeatureNotSupportedException("Method not supported")

  override def nullPlusNonNullIsNull(): Boolean = throw new SQLFeatureNotSupportedException("Method not supported")

  override def supportsConvert(): Boolean = throw new SQLFeatureNotSupportedException("Method not supported")

  override def supportsConvert(fromType: Int, toType: Int): Boolean = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def supportsTableCorrelationNames(): Boolean = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def supportsDifferentTableCorrelationNames(): Boolean = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def supportsExpressionsInOrderBy(): Boolean = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def supportsOrderByUnrelated(): Boolean = throw new SQLFeatureNotSupportedException("Method not supported")

  override def supportsGroupBy(): Boolean = throw new SQLFeatureNotSupportedException("Method not supported")

  override def supportsGroupByUnrelated(): Boolean = throw new SQLFeatureNotSupportedException("Method not supported")

  override def supportsGroupByBeyondSelect(): Boolean = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def supportsLikeEscapeClause(): Boolean = throw new SQLFeatureNotSupportedException("Method not supported")

  override def supportsMultipleResultSets(): Boolean = throw new SQLFeatureNotSupportedException("Method not supported")

  override def supportsMultipleTransactions(): Boolean = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def supportsNonNullableColumns(): Boolean = throw new SQLFeatureNotSupportedException("Method not supported")

  override def supportsMinimumSQLGrammar(): Boolean = throw new SQLFeatureNotSupportedException("Method not supported")

  override def supportsCoreSQLGrammar(): Boolean = throw new SQLFeatureNotSupportedException("Method not supported")

  override def supportsExtendedSQLGrammar(): Boolean = throw new SQLFeatureNotSupportedException("Method not supported")

  override def supportsANSI92EntryLevelSQL(): Boolean = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def supportsANSI92IntermediateSQL(): Boolean = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def supportsANSI92FullSQL(): Boolean = throw new SQLFeatureNotSupportedException("Method not supported")

  override def supportsIntegrityEnhancementFacility(): Boolean = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def supportsOuterJoins(): Boolean = throw new SQLFeatureNotSupportedException("Method not supported")

  override def supportsFullOuterJoins(): Boolean = throw new SQLFeatureNotSupportedException("Method not supported")

  override def supportsLimitedOuterJoins(): Boolean = throw new SQLFeatureNotSupportedException("Method not supported")

  override def getSchemaTerm: String = "database"

  override def getProcedureTerm: String = throw new SQLFeatureNotSupportedException("Method not supported")

  override def getCatalogTerm: String = throw new SQLFeatureNotSupportedException("Method not supported")

  override def isCatalogAtStart: Boolean = throw new SQLFeatureNotSupportedException("Method not supported")

  override def getCatalogSeparator: String = throw new SQLFeatureNotSupportedException("Method not supported")

  override def supportsSchemasInDataManipulation(): Boolean = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def supportsSchemasInProcedureCalls(): Boolean = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def supportsSchemasInTableDefinitions(): Boolean = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def supportsSchemasInIndexDefinitions(): Boolean = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def supportsSchemasInPrivilegeDefinitions(): Boolean = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def supportsCatalogsInDataManipulation(): Boolean = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def supportsCatalogsInProcedureCalls(): Boolean = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def supportsCatalogsInTableDefinitions(): Boolean = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def supportsCatalogsInIndexDefinitions(): Boolean = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def supportsCatalogsInPrivilegeDefinitions(): Boolean = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def supportsPositionedDelete(): Boolean = throw new SQLFeatureNotSupportedException("Method not supported")

  override def supportsPositionedUpdate(): Boolean = throw new SQLFeatureNotSupportedException("Method not supported")

  override def supportsSelectForUpdate(): Boolean = throw new SQLFeatureNotSupportedException("Method not supported")

  override def supportsStoredProcedures(): Boolean = throw new SQLFeatureNotSupportedException("Method not supported")

  override def supportsSubqueriesInComparisons(): Boolean = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def supportsSubqueriesInExists(): Boolean = throw new SQLFeatureNotSupportedException("Method not supported")

  override def supportsSubqueriesInIns(): Boolean = throw new SQLFeatureNotSupportedException("Method not supported")

  override def supportsSubqueriesInQuantifieds(): Boolean = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def supportsCorrelatedSubqueries(): Boolean = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def supportsUnion(): Boolean = throw new SQLFeatureNotSupportedException("Method not supported")

  override def supportsUnionAll(): Boolean = throw new SQLFeatureNotSupportedException("Method not supported")

  override def supportsOpenCursorsAcrossCommit(): Boolean = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def supportsOpenCursorsAcrossRollback(): Boolean = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def supportsOpenStatementsAcrossCommit(): Boolean = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def supportsOpenStatementsAcrossRollback(): Boolean = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def getMaxBinaryLiteralLength: Int = throw new SQLFeatureNotSupportedException("Method not supported")

  override def getMaxCharLiteralLength: Int = throw new SQLFeatureNotSupportedException("Method not supported")

  override def getMaxColumnNameLength: Int = throw new SQLFeatureNotSupportedException("Method not supported")

  override def getMaxColumnsInGroupBy: Int = throw new SQLFeatureNotSupportedException("Method not supported")

  override def getMaxColumnsInIndex: Int = throw new SQLFeatureNotSupportedException("Method not supported")

  override def getMaxColumnsInOrderBy: Int = throw new SQLFeatureNotSupportedException("Method not supported")

  override def getMaxColumnsInSelect: Int = throw new SQLFeatureNotSupportedException("Method not supported")

  override def getMaxColumnsInTable: Int = throw new SQLFeatureNotSupportedException("Method not supported")

  override def getMaxConnections: Int = throw new SQLFeatureNotSupportedException("Method not supported")

  override def getMaxCursorNameLength: Int = throw new SQLFeatureNotSupportedException("Method not supported")

  override def getMaxIndexLength: Int = throw new SQLFeatureNotSupportedException("Method not supported")

  override def getMaxSchemaNameLength: Int = throw new SQLFeatureNotSupportedException("Method not supported")

  override def getMaxProcedureNameLength: Int = throw new SQLFeatureNotSupportedException("Method not supported")

  override def getMaxCatalogNameLength: Int = throw new SQLFeatureNotSupportedException("Method not supported")

  override def getMaxRowSize: Int = throw new SQLFeatureNotSupportedException("Method not supported")

  override def doesMaxRowSizeIncludeBlobs(): Boolean = throw new SQLFeatureNotSupportedException("Method not supported")

  override def getMaxStatementLength: Int = throw new SQLFeatureNotSupportedException("Method not supported")

  override def getMaxStatements: Int = throw new SQLFeatureNotSupportedException("Method not supported")

  override def getMaxTableNameLength: Int = throw new SQLFeatureNotSupportedException("Method not supported")

  override def getMaxTablesInSelect: Int = throw new SQLFeatureNotSupportedException("Method not supported")

  override def getMaxUserNameLength: Int = throw new SQLFeatureNotSupportedException("Method not supported")

  override def getDefaultTransactionIsolation: Int = Connection.TRANSACTION_NONE

  override def supportsTransactions(): Boolean = false

  override def supportsTransactionIsolationLevel(level: Int): Boolean = level == Connection.TRANSACTION_NONE

  override def supportsDataDefinitionAndDataManipulationTransactions(): Boolean =
    throw new SQLFeatureNotSupportedException("Method not supported")

  override def supportsDataManipulationTransactionsOnly(): Boolean = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def dataDefinitionCausesTransactionCommit(): Boolean = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def dataDefinitionIgnoredInTransactions(): Boolean = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def getProcedures(catalog: String, schemaPattern: String, procedureNamePattern: String): ResultSet =
    throw new SQLFeatureNotSupportedException("Method not supported")

  override def getProcedureColumns(
    catalog: String,
    schemaPattern: String,
    procedureNamePattern: String,
    columnNamePattern: String
  ): ResultSet = throw new SQLFeatureNotSupportedException("Method not supported")

  override def getTables(
    catalog: String,
    schemaPattern: String,
    tableNamePattern: String,
    types: Array[String]
  ): ResultSet =
    val stmt = client.getTables(session, Option(schemaPattern).getOrElse("%"), tableNamePattern)
    BitlapQueryResultSet.builder().setClient(client).setStmtHandle(stmt).build()

  override def getSchemas: ResultSet =
    getSchemas(null, null)

  override def getCatalogs: ResultSet = throw new SQLFeatureNotSupportedException("Method not supported")

  override def getTableTypes: ResultSet = throw new SQLFeatureNotSupportedException("Method not supported")

  override def getColumns(
    catalog: String,
    schemaPattern: String,
    tableNamePattern: String,
    columnNamePattern: String
  ): ResultSet = throw new SQLFeatureNotSupportedException("Method not supported")

  override def getColumnPrivileges(
    catalog: String,
    schema: String,
    table: String,
    columnNamePattern: String
  ): ResultSet = throw new SQLFeatureNotSupportedException("Method not supported")

  override def getTablePrivileges(catalog: String, schemaPattern: String, tableNamePattern: String): ResultSet =
    throw new SQLFeatureNotSupportedException("Method not supported")

  override def getBestRowIdentifier(
    catalog: String,
    schema: String,
    table: String,
    scope: Int,
    nullable: Boolean
  ): ResultSet = throw new SQLFeatureNotSupportedException("Method not supported")

  override def getVersionColumns(catalog: String, schema: String, table: String): ResultSet =
    throw new SQLFeatureNotSupportedException("Method not supported")

  override def getPrimaryKeys(catalog: String, schema: String, table: String): ResultSet =
    throw new SQLFeatureNotSupportedException("Method not supported")

  override def getImportedKeys(catalog: String, schema: String, table: String): ResultSet =
    throw new SQLFeatureNotSupportedException("Method not supported")

  override def getExportedKeys(catalog: String, schema: String, table: String): ResultSet =
    throw new SQLFeatureNotSupportedException("Method not supported")

  override def getCrossReference(
    parentCatalog: String,
    parentSchema: String,
    parentTable: String,
    foreignCatalog: String,
    foreignSchema: String,
    foreignTable: String
  ): ResultSet = throw new SQLFeatureNotSupportedException("Method not supported")

  override def getTypeInfo: ResultSet = throw new SQLFeatureNotSupportedException("Method not supported")

  override def getIndexInfo(
    catalog: String,
    schema: String,
    table: String,
    unique: Boolean,
    approximate: Boolean
  ): ResultSet = throw new SQLFeatureNotSupportedException("Method not supported")

  override def supportsResultSetType(`type`: Int): Boolean = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def supportsResultSetConcurrency(`type`: Int, concurrency: Int): Boolean =
    throw new SQLFeatureNotSupportedException("Method not supported")

  override def ownUpdatesAreVisible(`type`: Int): Boolean = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def ownDeletesAreVisible(`type`: Int): Boolean = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def ownInsertsAreVisible(`type`: Int): Boolean = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def othersUpdatesAreVisible(`type`: Int): Boolean = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def othersDeletesAreVisible(`type`: Int): Boolean = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def othersInsertsAreVisible(`type`: Int): Boolean = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def updatesAreDetected(`type`: Int): Boolean = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def deletesAreDetected(`type`: Int): Boolean = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def insertsAreDetected(`type`: Int): Boolean = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def supportsBatchUpdates(): Boolean = throw new SQLFeatureNotSupportedException("Method not supported")

  override def getUDTs(
    catalog: String,
    schemaPattern: String,
    typeNamePattern: String,
    types: Array[Int]
  ): ResultSet =
    throw new SQLFeatureNotSupportedException("Method not supported")

  override def getConnection: Connection = this.connection

  override def supportsSavepoints(): Boolean = throw new SQLFeatureNotSupportedException("Method not supported")

  override def supportsNamedParameters(): Boolean = throw new SQLFeatureNotSupportedException("Method not supported")

  override def supportsMultipleOpenResults(): Boolean = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def supportsGetGeneratedKeys(): Boolean = throw new SQLFeatureNotSupportedException("Method not supported")

  override def getSuperTypes(catalog: String, schemaPattern: String, typeNamePattern: String): ResultSet =
    throw new SQLFeatureNotSupportedException("Method not supported")

  override def getSuperTables(catalog: String, schemaPattern: String, tableNamePattern: String): ResultSet =
    throw new SQLFeatureNotSupportedException("Method not supported")

  override def getAttributes(
    catalog: String,
    schemaPattern: String,
    typeNamePattern: String,
    attributeNamePattern: String
  ): ResultSet = throw new SQLFeatureNotSupportedException("Method not supported")

  override def supportsResultSetHoldability(holdability: Int): Boolean = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def getResultSetHoldability: Int = throw new SQLFeatureNotSupportedException("Method not supported")

  override def getDatabaseMajorVersion: Int = throw new SQLFeatureNotSupportedException("Method not supported")

  override def getDatabaseMinorVersion: Int = throw new SQLFeatureNotSupportedException("Method not supported")

  override def getJDBCMajorVersion: Int = throw new SQLFeatureNotSupportedException("Method not supported")

  override def getJDBCMinorVersion: Int = throw new SQLFeatureNotSupportedException("Method not supported")

  override def getSQLStateType: Int = DatabaseMetaData.sqlStateSQL99

  override def locatorsUpdateCopy(): Boolean = throw new SQLFeatureNotSupportedException("Method not supported")

  override def supportsStatementPooling(): Boolean = throw new SQLFeatureNotSupportedException("Method not supported")

  override def getRowIdLifetime: RowIdLifetime = throw new SQLFeatureNotSupportedException("Method not supported")

  override def getSchemas(catalog: String, schemaPattern: String): ResultSet =
    val stmt = client.getDatabases(session, Option(catalog).orElse(Option(schemaPattern)).getOrElse("%"))
    BitlapQueryResultSet.builder().setClient(client).setStmtHandle(stmt).build()

  override def supportsStoredFunctionsUsingCallSyntax(): Boolean = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def autoCommitFailureClosesAllResultSets(): Boolean = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def getClientInfoProperties: ResultSet = throw new SQLFeatureNotSupportedException("Method not supported")

  override def getFunctions(catalog: String, schemaPattern: String, functionNamePattern: String): ResultSet =
    throw new SQLFeatureNotSupportedException("Method not supported")

  override def getFunctionColumns(
    catalog: String,
    schemaPattern: String,
    functionNamePattern: String,
    columnNamePattern: String
  ): ResultSet = throw new SQLFeatureNotSupportedException("Method not supported")

  override def getPseudoColumns(
    catalog: String,
    schemaPattern: String,
    tableNamePattern: String,
    columnNamePattern: String
  ): ResultSet = throw new SQLFeatureNotSupportedException("Method not supported")

  override def generatedKeyAlwaysReturned(): Boolean = throw new SQLFeatureNotSupportedException("Method not supported")

  override def unwrap[T](iface: Class[T]): T = throw new SQLFeatureNotSupportedException("Method not supported")

  override def isWrapperFor(iface: Class[?]): Boolean = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )
}
