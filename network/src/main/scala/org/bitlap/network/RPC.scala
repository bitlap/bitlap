/* Copyright (c) 2022 bitlap.org */
package org.bitlap.network

import com.alipay.sofa.jraft.rpc.RpcRequestProcessor
import com.alipay.sofa.jraft.rpc.impl.MarshallerHelper
import com.alipay.sofa.jraft.util.RpcFactoryHelper
import org.bitlap.common.BitlapConf
import org.bitlap.network.proto.driver._

/**
 * RPC utils for network
 */
object RPC {

  def newClient(conf: BitlapConf, uri: String): RpcClient =
    new RpcClient(uri, conf)

  def newServer(conf: BitlapConf, processors: List[RpcRequestProcessor[_]]): RpcServer =
    new RpcServer(conf, processors)

  // register request serializer
  val requestInstances = List(
    classOf[BOpenSession.BOpenSessionReq] -> BOpenSession.BOpenSessionReq.getDefaultInstance,
    classOf[BCloseSession.BCloseSessionReq] -> BCloseSession.BCloseSessionReq.getDefaultInstance,
    classOf[BExecuteStatement.BExecuteStatementReq] -> BExecuteStatement.BExecuteStatementReq.getDefaultInstance,
    classOf[BFetchResults.BFetchResultsReq] -> BFetchResults.BFetchResultsReq.getDefaultInstance,
    classOf[BGetColumns.BGetColumnsReq] -> BGetColumns.BGetColumnsReq.getDefaultInstance,
    classOf[BGetSchemas.BGetSchemasReq] -> BGetSchemas.BGetSchemasReq.getDefaultInstance,
    classOf[BGetTables.BGetTablesReq] -> BGetTables.BGetTablesReq.getDefaultInstance,
    classOf[
      BGetResultSetMetadata.BGetResultSetMetadataReq
    ] -> BGetResultSetMetadata.BGetResultSetMetadataReq.getDefaultInstance
  )
  requestInstances.foreach { case (i1, i2) => RpcFactoryHelper.rpcFactory().registerProtobufSerializer(i1.getName, i2) }

  // register response serializer
  val responseInstances = List(
    classOf[BOpenSession.BOpenSessionReq] -> BOpenSession.BOpenSessionResp.getDefaultInstance,
    classOf[BCloseSession.BCloseSessionReq] -> BCloseSession.BCloseSessionResp.getDefaultInstance,
    classOf[BExecuteStatement.BExecuteStatementReq] -> BExecuteStatement.BExecuteStatementResp.getDefaultInstance,
    classOf[BFetchResults.BFetchResultsReq] -> BFetchResults.BFetchResultsResp.getDefaultInstance,
    classOf[BGetColumns.BGetColumnsReq] -> BGetColumns.BGetColumnsResp.getDefaultInstance,
    classOf[BGetSchemas.BGetSchemasReq] -> BGetSchemas.BGetSchemasResp.getDefaultInstance,
    classOf[BGetTables.BGetTablesReq] -> BGetTables.BGetTablesResp.getDefaultInstance,
    classOf[
      BGetResultSetMetadata.BGetResultSetMetadataReq
    ] -> BGetResultSetMetadata.BGetResultSetMetadataResp.getDefaultInstance
  )
  responseInstances.foreach { case (i1, i2) => MarshallerHelper.registerRespInstance(i1.getName, i2) }
}
