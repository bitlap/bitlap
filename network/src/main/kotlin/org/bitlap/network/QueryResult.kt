package org.bitlap.network

import org.bitlap.network.core.RowSet
import org.bitlap.network.core.TableSchema

data class QueryResult(val tableSchema: TableSchema, val rows: RowSet)
