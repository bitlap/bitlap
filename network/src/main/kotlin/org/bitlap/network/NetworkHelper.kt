package org.bitlap.network

import com.google.protobuf.GeneratedMessageV3
import org.bitlap.network.proto.driver.BCloseSession
import org.bitlap.network.proto.driver.BExecuteStatement
import org.bitlap.network.proto.driver.BFetchResults
import org.bitlap.network.proto.driver.BGetColumns
import org.bitlap.network.proto.driver.BGetResultSetMetadata
import org.bitlap.network.proto.driver.BGetSchemas
import org.bitlap.network.proto.driver.BGetTables
import org.bitlap.network.proto.driver.BOpenSession

/**
 * RPC registration helper class.
 *
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
                    BOpenSession.BOpenSessionReq::class.java.name,
                    BOpenSession.BOpenSessionResp.getDefaultInstance()
                ),
                Pair(
                    BCloseSession.BCloseSessionReq::class.java.name,
                    BCloseSession.BCloseSessionResp.getDefaultInstance()
                ),
                Pair(
                    BExecuteStatement.BExecuteStatementReq::class.java.name,
                    BExecuteStatement.BExecuteStatementResp.getDefaultInstance()
                ),
                Pair(
                    BFetchResults.BFetchResultsReq::class.java.name,
                    BFetchResults.BFetchResultsResp.getDefaultInstance()
                ),
                Pair(
                    BGetColumns.BGetColumnsReq::class.java.name,
                    BGetColumns.BGetColumnsResp.getDefaultInstance()
                ),
                Pair(
                    BGetSchemas.BGetSchemasReq::class.java.name,
                    BGetSchemas.BGetSchemasResp.getDefaultInstance()
                ),
                Pair(
                    BGetTables.BGetTablesReq::class.java.name,
                    BGetTables.BGetTablesResp.getDefaultInstance()
                ),
                Pair(
                    BGetResultSetMetadata.BGetResultSetMetadataReq::class.java.name,
                    BGetResultSetMetadata.BGetResultSetMetadataResp.getDefaultInstance()
                ),
            )
        }

        fun requestInstances(): List<Pair<String, GeneratedMessageV3>> {
            return listOf(
                Pair(
                    BOpenSession.BOpenSessionReq::class.java.name,
                    BOpenSession.BOpenSessionReq.getDefaultInstance()
                ),
                Pair(
                    BCloseSession.BCloseSessionReq::class.java.name,
                    BCloseSession.BCloseSessionReq.getDefaultInstance()
                ),
                Pair(
                    BExecuteStatement.BExecuteStatementReq::class.java.name,
                    BExecuteStatement.BExecuteStatementReq.getDefaultInstance()
                ),
                Pair(
                    BFetchResults.BFetchResultsReq::class.java.name,
                    BFetchResults.BFetchResultsReq.getDefaultInstance()
                ),
                Pair(
                    BGetColumns.BGetColumnsReq::class.java.name,
                    BGetColumns.BGetColumnsReq.getDefaultInstance()
                ),
                Pair(
                    BGetSchemas.BGetSchemasReq::class.java.name,
                    BGetSchemas.BGetSchemasReq.getDefaultInstance()
                ),
                Pair(
                    BGetTables.BGetTablesReq::class.java.name,
                    BGetTables.BGetTablesReq.getDefaultInstance()
                ),
                Pair(
                    BGetResultSetMetadata.BGetResultSetMetadataReq::class.java.name,
                    BGetResultSetMetadata.BGetResultSetMetadataReq.getDefaultInstance()
                ),
            )
        }
    }
}
