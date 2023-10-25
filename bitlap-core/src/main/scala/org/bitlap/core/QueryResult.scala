/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core

import java.sql.ResultSet

/** Desc: query result
 */
final case class QueryResult(data: ResultSet, currentSchema: String)
