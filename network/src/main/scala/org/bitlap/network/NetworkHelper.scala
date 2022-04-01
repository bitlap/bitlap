/* Copyright (c) 2022 bitlap.org */
package org.bitlap.network

import com.google.protobuf.GeneratedMessageV3
import org.bitlap.network.proto.driver._

/**
 * @author 梦境迷离
 * @since 2021/11/20
 * @version 1.0
 */
trait NetworkHelper {

  def registerMessageInstances(
    service: List[(String, GeneratedMessageV3)],
    action: (String, GeneratedMessageV3) => Unit
  ): Unit =
    service.foreach(kv => action(kv._1, kv._2))

}

object NetworkHelper {
  def responseInstances(): List[(String, GeneratedMessageV3)] =
    List(
      classOf[
        BOpenSession.BOpenSessionReq
      ].getName -> BOpenSession.BOpenSessionResp.getDefaultInstance,
      classOf[
        BCloseSession.BCloseSessionReq
      ].getName -> BCloseSession.BCloseSessionResp.getDefaultInstance,
      classOf[
        BExecuteStatement.BExecuteStatementReq
      ].getName -> BExecuteStatement.BExecuteStatementResp.getDefaultInstance,
      classOf[
        BFetchResults.BFetchResultsReq
      ].getName -> BFetchResults.BFetchResultsResp.getDefaultInstance,
      classOf[
        BGetColumns.BGetColumnsReq
      ].getName -> BGetColumns.BGetColumnsResp.getDefaultInstance,
      classOf[
        BGetSchemas.BGetSchemasReq
      ].getName -> BGetSchemas.BGetSchemasResp.getDefaultInstance,
      classOf[
        BGetTables.BGetTablesReq
      ].getName -> BGetTables.BGetTablesResp.getDefaultInstance,
      classOf[
        BGetResultSetMetadata.BGetResultSetMetadataReq
      ].getName -> BGetResultSetMetadata.BGetResultSetMetadataResp.getDefaultInstance
    )

  def requestInstances(): List[(String, GeneratedMessageV3)] =
    List(
      classOf[
        BOpenSession.BOpenSessionReq
      ].getName -> BOpenSession.BOpenSessionReq.getDefaultInstance,
      classOf[
        BCloseSession.BCloseSessionReq
      ].getName -> BCloseSession.BCloseSessionReq.getDefaultInstance,
      classOf[
        BExecuteStatement.BExecuteStatementReq
      ].getName -> BExecuteStatement.BExecuteStatementReq.getDefaultInstance,
      classOf[
        BFetchResults.BFetchResultsReq
      ].getName -> BFetchResults.BFetchResultsReq.getDefaultInstance,
      classOf[
        BGetColumns.BGetColumnsReq
      ].getName -> BGetColumns.BGetColumnsReq.getDefaultInstance,
      classOf[
        BGetSchemas.BGetSchemasReq
      ].getName -> BGetSchemas.BGetSchemasReq.getDefaultInstance,
      classOf[
        BGetTables.BGetTablesReq
      ].getName -> BGetTables.BGetTablesReq.getDefaultInstance,
      classOf[
        BGetResultSetMetadata.BGetResultSetMetadataReq
      ].getName -> BGetResultSetMetadata.BGetResultSetMetadataReq.getDefaultInstance
    )
}
