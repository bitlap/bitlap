package org.bitlap.network.core.operation

import org.bitlap.common.logger
import org.bitlap.network.QueryResult
import org.bitlap.network.core.OperationType
import org.bitlap.network.core.RowSet
import org.bitlap.network.core.Session
import org.bitlap.network.core.TableSchema

/**
 * This class should abstract according to different operations, and each operation is a special implementation.
 *
 * @author 梦境迷离
 * @since 2021/9/5
 * @version 1.0
 */
abstract class Operation(val parentSession: Session, val opType: OperationType, val hasResultSet: Boolean = false) {
    protected val log = logger { }

    val opHandle: OperationHandle by lazy { OperationHandle(opType, hasResultSet) }

    lateinit var statement: String
    var confOverlay: Map<String, String>? = mutableMapOf()

    abstract fun run()

    fun remove(operationHandle: OperationHandle) {
        cache.remove(operationHandle)
    }

    protected val cache: MutableMap<OperationHandle, QueryResult> = mutableMapOf()

    fun getNextResultSet(): RowSet {
        return cache[opHandle]?.rows ?: RowSet()
    }

    fun getResultSetSchema(): TableSchema {
        return cache[opHandle]?.tableSchema ?: TableSchema()
    }
}
