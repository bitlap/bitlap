package org.bitlap.server.raft.cli

import java.util.UUID
import org.bitlap.common.proto.driver.BHandleIdentifier
import org.bitlap.common.proto.driver.BOperationHandle
import org.bitlap.common.proto.driver.BOperationType

/**
 * Implementation of driver RPC core.
 * @author 梦境迷离
 * @since 2021/6/6
 * @version 1.0
 */
open class BCLIService(private val sessionManager: SessionManager) : CLIService {

    // get session manager by conf
    // create session
    // execute statement by session
    // return OperationHandle
    // fetch results by OperationHandle

    override fun openSession(
        username: String,
        password: String,
        configuration: Map<String, String>?
    ): SessionHandle? {
        return sessionManager.openSession(
            null, username, password, configuration ?: mapOf()
        ).sessionHandle
    }

    override fun closeSession(sessionHandle: SessionHandle) {
        sessionManager.closeSession(sessionHandle)
    }

    override fun executeStatement(
        sessionHandle: SessionHandle,
        statement: String,
        confOverlay: Map<String, String>?
    ): OperationHandle? {
        val opHandle = BOperationHandle.newBuilder().setOperationType(BOperationType.B_OPERATION_TYPE_EXECUTE_STATEMENT)
            .setOperationId(
                BHandleIdentifier.newBuilder().setGuid(UUID.randomUUID().toString())
                    .setSecret(UUID.randomUUID().toString()).build()
            )
            .setHasResultSet(true).build()
        return OperationHandle(opHandle)
    }

    override fun executeStatement(
        sessionHandle: SessionHandle,
        statement: String,
        confOverlay: Map<String, String>?,
        queryTimeout: Long
    ): OperationHandle? {
        val opHandle = BOperationHandle.newBuilder().setOperationType(BOperationType.B_OPERATION_TYPE_EXECUTE_STATEMENT)
            .setOperationId(
                BHandleIdentifier.newBuilder().setGuid(UUID.randomUUID().toString())
                    .setSecret(UUID.randomUUID().toString()).build()
            )
            .setHasResultSet(true).build()
        return OperationHandle(opHandle)
    }

    override fun fetchResults(opHandle: OperationHandle): List<String?>? {
        return listOf("hello", "world", "nice", "to", "meet", "you") //mock data
    }
}
