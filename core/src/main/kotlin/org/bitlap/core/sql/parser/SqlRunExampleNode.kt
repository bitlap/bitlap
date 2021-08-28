package org.bitlap.core.sql

import org.apache.calcite.sql.BitlapSqlNode
import org.apache.calcite.sql.SqlNode
import org.apache.calcite.sql.SqlWriter
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.sql.util.SqlVisitor
import org.apache.calcite.sql.validate.SqlValidator
import org.apache.calcite.sql.validate.SqlValidatorScope
import org.apache.calcite.util.Litmus

/**
 * Desc:
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/7/28
 */
class SqlRunExampleNode(val pos: SqlParserPos, val exampleString: String) : BitlapSqlNode(pos) {

    override fun clone(sqlParserPos: SqlParserPos?): SqlNode? {
        println("clone")
        return null
    }

    override fun unparse(sqlWriter: SqlWriter, i: Int, i1: Int) {
        sqlWriter.keyword("run")
        sqlWriter.keyword("example")
        sqlWriter.print("\n")
        sqlWriter.keyword(exampleString)
    }

    override fun validate(sqlValidator: SqlValidator, sqlValidatorScope: SqlValidatorScope) {
        println("validate")
    }

    override fun <R> accept(sqlVisitor: SqlVisitor<R>?): R? {
        println("validate")
        return null
    }

    override fun equalsDeep(sqlNode: SqlNode?, litmus: Litmus?): Boolean {
        println("equalsDeep")
        return false
    }
}
