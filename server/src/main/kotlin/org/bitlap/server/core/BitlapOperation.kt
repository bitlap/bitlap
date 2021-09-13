package org.bitlap.server.core

import com.google.protobuf.ByteString
import org.bitlap.core.sql.QueryExecution
import org.bitlap.network.QueryResult
import org.bitlap.network.core.ColumnDesc
import org.bitlap.network.core.OperationType
import org.bitlap.network.core.Row
import org.bitlap.network.core.RowSet
import org.bitlap.network.core.Session
import org.bitlap.network.core.TableSchema
import org.bitlap.network.core.TypeId
import org.bitlap.network.core.operation.Operation
import java.sql.ResultSet
import java.sql.Types

/**
 *
 * @author 梦境迷离
 * @since 2021/9/5
 * @version 1.0
 */
class BitlapOperation(parentSession: Session, opType: OperationType, hasResultSet: Boolean = false) :
    Operation(parentSession, opType, hasResultSet) {

    override fun run() {
        cache[super.opHandle] = wrapper(QueryExecution(super.statement).execute())
    }

    private fun wrapper(rs: ResultSet): QueryResult {
        rs.use {
            // get schema
            val metaData = rs.metaData
            val columns = (1..metaData.columnCount).map {
                val colName = metaData.getColumnName(it)
                val colType = when (metaData.getColumnType(it)) {
                    Types.VARCHAR -> TypeId.B_TYPE_ID_STRING_TYPE
                    Types.INTEGER -> TypeId.B_TYPE_ID_INT_TYPE
                    Types.DOUBLE -> TypeId.B_TYPE_ID_DOUBLE_TYPE
                    // TODO more
                    else -> TypeId.B_TYPE_ID_UNSPECIFIED
                }
                ColumnDesc(colName, colType)
            }
            // get row set
            val rows = mutableListOf<Row>()
            while (rs.next()) {
                val cols = (1..metaData.columnCount).map {
                    when (metaData.getColumnType(it)) {
                        Types.VARCHAR -> ByteString.copyFromUtf8(rs.getString(it))
                        // TODO more
                        else -> ByteString.copyFrom(rs.getBytes(it))
                    }
                }
                rows.add(Row(cols))
            }
            return QueryResult(TableSchema(columns), RowSet(rows))
        }
    }
}
