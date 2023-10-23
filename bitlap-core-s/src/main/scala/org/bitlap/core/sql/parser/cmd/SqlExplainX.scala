/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.sql.parser.cmd

import org.apache.calcite.DataContext
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.SqlNode
import org.apache.calcite.sql.SqlSelect
import org.apache.calcite.sql.SqlSpecialOperator
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.sql.`type`.SqlTypeName
import org.bitlap.core.BitlapContext
import org.bitlap.core.sql.parser.BitlapSqlDdlNode

/**
 * Desc: explain a sql query
 */
class SqlExplainX(
    override val _pos: SqlParserPos,
    private val stmt: SqlNode,
) extends BitlapSqlDdlNode(_pos, SqlExplainX.OPERATOR, List(stmt)) {

    override val resultTypes: List[(String, SqlTypeName)]
        = List(
            "result" -> SqlTypeName.VARCHAR,
        )

    override def operator(context: DataContext): List[Array[Any]] = {
        return stmt match {
            case _: SqlSelect => {
                val plan = BitlapContext.sqlPlanner.parse(stmt.toString)
                List(Array(plan.explain()))
            }
            case _ => {
                throw IllegalArgumentException(s"Illegal explain SqlSelect node $stmt")
            }
        }
    }
}

object SqlExplainX {
  val OPERATOR = SqlSpecialOperator("EXPLAIN", SqlKind.OTHER)
}
