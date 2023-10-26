/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core

import java.sql.ResultSet

/** query result
 */
sealed trait QueryResult

final case class DefaultQueryResult(data: ResultSet, currentSchema: String) extends QueryResult
