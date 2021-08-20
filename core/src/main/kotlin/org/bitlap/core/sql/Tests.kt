package org.bitlap.core.sql

import org.apache.calcite.config.Lex
import org.apache.calcite.sql.dialect.OracleSqlDialect
import org.apache.calcite.sql.parser.SqlParser

/**
 * Desc:
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2020/11/25
 */
fun main1() {
    // Sql语句
    val sql = "select * from emps where id = 1"
    // 解析配置
    val mysqlConfig = SqlParser.config().withLex(Lex.MYSQL)
    // 创建解析器
    val parser = SqlParser.create(sql, mysqlConfig)
    // 解析sql
    val sqlNode = parser.parseQuery()
    // 还原某个方言的SQL
    println(sqlNode.toSqlString(OracleSqlDialect.DEFAULT))
}

fun main() {
//    val sql = "select 1+2*3, id, name from test where id < 5 and name = 'mimosa'"
    val sql = "create schema a"
    val parser = QueryExecution(sql)
}
