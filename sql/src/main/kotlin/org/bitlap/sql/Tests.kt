package org.bitlap.sql

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
fun main() {
    // Sql语句
    val sql = "select * from emps where id = 1"
    // 解析配置
    val mysqlConfig = SqlParser.configBuilder().setLex(Lex.MYSQL).build()
    // 创建解析器
    val parser = SqlParser.create(sql, mysqlConfig)
    // 解析sql
    val sqlNode = parser.parseQuery()
    // 还原某个方言的SQL
    println(sqlNode.toSqlString(OracleSqlDialect.DEFAULT))
}