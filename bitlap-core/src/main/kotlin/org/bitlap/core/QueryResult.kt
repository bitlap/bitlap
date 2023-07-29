/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core

import java.sql.ResultSet

/**
 * Desc: query result
 */
data class QueryResult(
    val data: ResultSet,
    val currentSchema: String
)
