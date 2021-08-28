package org.bitlap.network

import com.google.protobuf.GeneratedMessageV3

/**
 * rpc network context and action
 * @author 梦境迷离
 * @since 2021/6/13
 * @version 1.0
 */
interface NetworkHelper {

    fun registerMessageInstances(
        service: List<Pair<String, GeneratedMessageV3>>,
        action: (Pair<String, GeneratedMessageV3>) -> Unit
    ) {
        service.forEach { action(it) }
    }

    companion object {
        fun responseInstances(): List<Pair<String, GeneratedMessageV3>> {
            return listOf(
                Pair(
                    org.bitlap.network.proto.driver.BOpenSession.BOpenSessionReq::class.java.name,
                    org.bitlap.network.proto.driver.BOpenSession.BOpenSessionResp.getDefaultInstance()
                ),
                Pair(
                    org.bitlap.network.proto.driver.BCloseSession.BCloseSessionReq::class.java.name,
                    org.bitlap.network.proto.driver.BCloseSession.BCloseSessionResp.getDefaultInstance()
                ),
                Pair(
                    org.bitlap.network.proto.driver.BExecuteStatement.BExecuteStatementReq::class.java.name,
                    org.bitlap.network.proto.driver.BExecuteStatement.BExecuteStatementResp.getDefaultInstance()
                ),
                Pair(
                    org.bitlap.network.proto.driver.BFetchResults.BFetchResultsReq::class.java.name,
                    org.bitlap.network.proto.driver.BFetchResults.BFetchResultsResp.getDefaultInstance()
                ),
                Pair(
                    org.bitlap.network.proto.driver.BGetColumns.BGetColumnsReq::class.java.name,
                    org.bitlap.network.proto.driver.BGetColumns.BGetColumnsResp.getDefaultInstance()
                ),
                Pair(
                    org.bitlap.network.proto.driver.BGetSchemas.BGetSchemasReq::class.java.name,
                    org.bitlap.network.proto.driver.BGetSchemas.BGetSchemasResp.getDefaultInstance()
                ),
                Pair(
                    org.bitlap.network.proto.driver.BGetTables.BGetTablesReq::class.java.name,
                    org.bitlap.network.proto.driver.BGetTables.BGetTablesResp.getDefaultInstance()
                ),
                Pair(
                    org.bitlap.network.proto.driver.BGetResultSetMetadata.BGetResultSetMetadataReq::class.java.name,
                    org.bitlap.network.proto.driver.BGetResultSetMetadata.BGetResultSetMetadataResp.getDefaultInstance()
                ),
            )
        }

        fun requestInstances(): List<Pair<String, GeneratedMessageV3>> {
            return listOf(
                Pair(
                    org.bitlap.network.proto.driver.BOpenSession.BOpenSessionReq::class.java.name,
                    org.bitlap.network.proto.driver.BOpenSession.BOpenSessionReq.getDefaultInstance()
                ),
                Pair(
                    org.bitlap.network.proto.driver.BCloseSession.BCloseSessionReq::class.java.name,
                    org.bitlap.network.proto.driver.BCloseSession.BCloseSessionReq.getDefaultInstance()
                ),
                Pair(
                    org.bitlap.network.proto.driver.BExecuteStatement.BExecuteStatementReq::class.java.name,
                    org.bitlap.network.proto.driver.BExecuteStatement.BExecuteStatementReq.getDefaultInstance()
                ),
                Pair(
                    org.bitlap.network.proto.driver.BFetchResults.BFetchResultsReq::class.java.name,
                    org.bitlap.network.proto.driver.BFetchResults.BFetchResultsReq.getDefaultInstance()
                ),
                Pair(
                    org.bitlap.network.proto.driver.BGetColumns.BGetColumnsReq::class.java.name,
                    org.bitlap.network.proto.driver.BGetColumns.BGetColumnsReq.getDefaultInstance()
                ),
                Pair(
                    org.bitlap.network.proto.driver.BGetSchemas.BGetSchemasReq::class.java.name,
                    org.bitlap.network.proto.driver.BGetSchemas.BGetSchemasReq.getDefaultInstance()
                ),
                Pair(
                    org.bitlap.network.proto.driver.BGetTables.BGetTablesReq::class.java.name,
                    org.bitlap.network.proto.driver.BGetTables.BGetTablesReq.getDefaultInstance()
                ),
                Pair(
                    org.bitlap.network.proto.driver.BGetResultSetMetadata.BGetResultSetMetadataReq::class.java.name,
                    org.bitlap.network.proto.driver.BGetResultSetMetadata.BGetResultSetMetadataReq.getDefaultInstance()
                ),
            )
        }
    }
}
