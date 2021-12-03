package org.bitlap.net

import com.alipay.sofa.jraft.RouteTable
import com.alipay.sofa.jraft.conf.Configuration
import com.alipay.sofa.jraft.option.CliOptions
import com.alipay.sofa.jraft.rpc.InvokeCallback
import com.alipay.sofa.jraft.rpc.impl.MarshallerHelper
import com.alipay.sofa.jraft.rpc.impl.cli.CliClientServiceImpl
import com.alipay.sofa.jraft.util.RpcFactoryHelper
import org.bitlap.common.BitlapConf
import org.bitlap.network.proto.driver.{ BCloseSession, BExecuteStatement, BFetchResults, BGetColumns, BGetResultSetMetadata, BGetSchemas, BGetTables, BOpenSession, BOperationHandle, BSessionHandle, BStatus, BStatusCode, BTableSchema }

import java.sql.SQLException
import java.util.Properties

/**
 * This class mainly wraps the RPC call procedure used inside JDBC.
 *
 * @author 梦境迷离
 * @since 2021/11/21
 * @version 1.0
 */
object BitlapClient extends NetworkHelper {

  //TODO remove kotlin
  private val group = "bitlap-cluster"
  private lazy val defaultConf = new BitlapConf()
  private lazy val bc = new BitlapConf.Companion()
  //  private lazy val groupId: String = defaultConf.get(group, bc.getNODE_GROUP_ID.getKey)
  //  private lazy val timeout: Long = defaultConf.get(group, bc.getNODE_RPC_TIMEOUT.getKey).toLong * 1000
  //  private lazy val raftTimeout: Int = defaultConf.get(group, bc.getNODE_RAFT_TIMEOUT.getKey).toInt * 1000
  private val groupId = "bitlap-cluster"
  private val timeout = 3000
  private val raftTimeout = 3000

  // kotlin 兼容
  def openSession(conf: Configuration, info: Properties)(implicit cc: CliClientServiceImpl): BSessionHandle = {
    cc.openSession(conf, info)
  }

  def closeSession(sessionHandle: BSessionHandle)(implicit cc: CliClientServiceImpl): Unit = {
    cc.closeSession(sessionHandle)
  }

  def executeStatement(sessionHandle: BSessionHandle, statement: String)(implicit cc: CliClientServiceImpl): BOperationHandle = {
    cc.executeStatement(sessionHandle, statement)
  }

  def fetchResults(operationHandle: BOperationHandle)(implicit cc: CliClientServiceImpl): BFetchResults.BFetchResultsResp = {
    cc.fetchResults(operationHandle)
  }

  def getSchemas(sessionHandle: BSessionHandle, catalogName: String = null, schemaName: String = null)
    (implicit cc: CliClientServiceImpl): BOperationHandle = {
    cc.getSchemas(sessionHandle, catalogName, schemaName)
  }

  def getTables(sessionHandle: BSessionHandle, catalogName: String = null, schemaName: String = null)
    (implicit cc: CliClientServiceImpl): BOperationHandle = {
    cc.getTables(sessionHandle, catalogName, schemaName)
  }

  def getColumns(sessionHandle: BSessionHandle, tableName: String = null, schemaName: String = null, columnName: String = null)
    (implicit cc: CliClientServiceImpl): BOperationHandle = {
    cc.getColumns(sessionHandle, tableName, schemaName, columnName)
  }

  /**
   * Used for JDBC to get Schema of the specified operation.
   */
  def getResultSetMetadata(operationHandle: BOperationHandle)(implicit cc: CliClientServiceImpl): BTableSchema = {
    cc.getResultSetMetadata(operationHandle)
  }


  implicit class CliClientServiceImplWrapper(val cc: CliClientServiceImpl) {

    /**
     * Used to open a session during JDBC connection initialization.
     */
    def openSession(conf: Configuration, info: Properties): BSessionHandle = {
      init(conf)
      val leader = RouteTable.getInstance().selectLeader(groupId)
      val result = cc.getRpcClient.invokeSync(
        leader.getEndpoint,
        BOpenSession.BOpenSessionReq.newBuilder().setUsername(info.getProperty("user"))
          .setPassword(info.getProperty("password")).build(),
        timeout
      )
      val re = result.asInstanceOf[BOpenSession.BOpenSessionResp]
      verifySuccess(re.getStatus)
      re.getSessionHandle
    }

    /**
     * Used to close the session when the JDBC connection is closed.
     */
    def closeSession(sessionHandle: BSessionHandle) {
      val leader = RouteTable.getInstance().selectLeader(groupId)
      cc.getRpcClient.invokeAsync(
        leader.getEndpoint,
        BCloseSession.BCloseSessionReq.newBuilder().setSessionHandle(sessionHandle).build(),
        new InvokeCallback() {
          override def complete(o: Any, throwable: Throwable) = {
            ()
          }
        }, timeout)
    }

    /**
     * Used to execute normal SQL by JDBC. Does not contain `?` placeholders.
     */
    def executeStatement(sessionHandle: BSessionHandle, statement: String): BOperationHandle = {
      val leader = RouteTable.getInstance().selectLeader(groupId)
      val result = cc.getRpcClient.invokeSync(
        leader.getEndpoint,
        BExecuteStatement.BExecuteStatementReq.newBuilder().setSessionHandle(sessionHandle).setStatement(statement)
          .build(),
        timeout
      )
      val re = result.asInstanceOf[BExecuteStatement.BExecuteStatementResp]
      verifySuccess(re.getStatus)
      re.getOperationHandle
    }

    /**
     * Used for JDBC to get result set of the specified operation.
     */
    def fetchResults(operationHandle: BOperationHandle): BFetchResults.BFetchResultsResp = {
      val leader = RouteTable.getInstance().selectLeader(groupId)
      val result = cc.getRpcClient.invokeSync(
        leader.getEndpoint,
        BFetchResults.BFetchResultsReq.newBuilder().setOperationHandle(operationHandle).build(),
        timeout
      )
      val re = result.asInstanceOf[BFetchResults.BFetchResultsResp]
      verifySuccess(re.getStatus)
      re
    }

    def getSchemas(sessionHandle: BSessionHandle, catalogName: String = null, schemaName: String = null): BOperationHandle = {
      val leader = RouteTable.getInstance().selectLeader(groupId)
      val result = cc.getRpcClient.invokeSync(
        leader.getEndpoint,
        BGetSchemas.BGetSchemasReq.newBuilder().setSessionHandle(sessionHandle).setCatalogName(catalogName)
          .setSchemaName(schemaName).build(),
        timeout
      )
      val re = result.asInstanceOf[BGetSchemas.BGetSchemasResp]
      verifySuccess(re.getStatus)
      re.getOperationHandle
    }

    def getTables(sessionHandle: BSessionHandle, tableName: String = null, schemaName: String = null): BOperationHandle = {
      val leader = RouteTable.getInstance().selectLeader(groupId)
      val result = cc.getRpcClient.invokeSync(
        leader.getEndpoint,
        BGetTables.BGetTablesReq.newBuilder().setSessionHandle(sessionHandle).setTableName(tableName)
          .setSchemaName(schemaName).build(),
        timeout
      )
      val re = result.asInstanceOf[BGetTables.BGetTablesResp]
      verifySuccess(re.getStatus)
      re.getOperationHandle
    }

    def getColumns(sessionHandle: BSessionHandle, tableName: String = null, schemaName: String = null, columnName: String = null): BOperationHandle = {
      val leader = RouteTable.getInstance().selectLeader(groupId)
      val result = cc.getRpcClient.invokeSync(
        leader.getEndpoint,
        BGetColumns.BGetColumnsReq.newBuilder().setSessionHandle(sessionHandle).setTableName(tableName)
          .setColumnName(columnName)
          .setSchemaName(schemaName).build(),
        timeout
      )
      val re = result.asInstanceOf[BGetColumns.BGetColumnsResp]
      verifySuccess(re.getStatus)
      re.getOperationHandle
    }

    /**
     * Used for JDBC to get Schema of the specified operation.
     */
    def getResultSetMetadata(operationHandle: BOperationHandle): BTableSchema = {
      val leader = RouteTable.getInstance().selectLeader(groupId)
      val result = cc.getRpcClient.invokeSync(
        leader.getEndpoint,
        BGetResultSetMetadata.BGetResultSetMetadataReq.newBuilder().setOperationHandle(operationHandle).build(),
        timeout
      )
      val re = result.asInstanceOf[BGetResultSetMetadata.BGetResultSetMetadataResp]
      verifySuccess(re.getStatus)
      re.getSchema
    }

    /**
     * Used to initialize available nodes.
     */
    private def init(conf: Configuration) {
      RouteTable.getInstance().updateConfiguration(groupId, conf)
      cc.getRpcClient.init(new CliOptions())
      assert(RouteTable.getInstance().refreshLeader(cc, groupId, raftTimeout).isOk, "Refresh leader failed")
    }

    /**
     * Used to verify whether the RPC result is correct.
     */
    private def verifySuccess(status: BStatus) {
      if (status.getStatusCode != BStatusCode.B_STATUS_CODE_SUCCESS_STATUS) {
        throw new SQLException(
          status.getErrorMessage,
          status.getSqlState, status.getErrorCode
        )
      }
    }
  }

  def beforeInit() {
    registerMessageInstances(NetworkHelper.requestInstances(), (i1, i2) => {
      RpcFactoryHelper.rpcFactory().registerProtobufSerializer(i1, i2)
    }
    )
    registerMessageInstances(NetworkHelper.responseInstances(), (i1, i2) => {
      MarshallerHelper.registerRespInstance(i1, i2)
    }
    )
  }

}
