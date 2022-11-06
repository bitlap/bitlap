/* Copyright (c) 2022 bitlap.org */
package org.bitlap.core.sql

import org.apache.calcite.sql.SqlNode
import org.apache.calcite.sql.SqlSelect
import org.bitlap.common.BitlapConf
import org.bitlap.core.SessionId
import java.io.Serializable

/**
 * Query thread context for each query statement.
 */
class QueryContext : Serializable {

    var runtimeConf: BitlapConf? = null
    var statement: String? = null
    var originalPlan: SqlNode? = null
    var currentSelectNode: SqlSelect? = null
    
    @Volatile var sessionId: SessionId? = null

    companion object {
        private val context = object : ThreadLocal<QueryContext>() {
            override fun initialValue(): QueryContext = QueryContext()
        }

        fun get(): QueryContext = context.get()
        
        fun reset() = context.remove()

        fun <T> use(block: (QueryContext) -> T): T {
            try {
                reset()
                return block(get())
            } finally {
                reset()
            }
        }
    }
}
