package org.bitlap.core.sql

import org.apache.calcite.adapter.java.AbstractQueryableTable
import org.apache.calcite.avatica.util.Casing
import org.apache.calcite.avatica.util.Quoting
import org.apache.calcite.linq4j.QueryProvider
import org.apache.calcite.linq4j.Queryable
import org.apache.calcite.rel.type.RelDataType
import org.apache.calcite.rel.type.RelDataTypeFactory
import org.apache.calcite.schema.SchemaPlus
import org.apache.calcite.sql.parser.SqlParser
import org.apache.calcite.sql.parser.bitlap.BitlapSqlParserImpl
import org.apache.calcite.sql.type.SqlTypeName
import org.apache.calcite.tools.Frameworks

/**
 * Desc:
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/8/6
 */
class QueryExecution(private val sql: String) {

    init {
        val schema = Frameworks.createRootSchema(true)
        schema.add(
            "test",
            object : AbstractQueryableTable(Object::class.java) {
                override fun getRowType(typeFactory: RelDataTypeFactory): RelDataType {
                    return typeFactory.builder()
                        .add("id", SqlTypeName.BIGINT)
                        .add("name", SqlTypeName.VARCHAR)
                        .build()
                }

                /** Converts this table into a [Queryable].  */
                override fun <T : Any?> asQueryable(queryProvider: QueryProvider, schema: SchemaPlus, tableName: String): Queryable<T> {
                    TODO("Not yet implemented")
                }
            }
        )

        val config = Frameworks.newConfigBuilder()
            .parserConfig(
                SqlParser.config()
                    .withParserFactory(BitlapSqlParserImpl.FACTORY)
                    .withCaseSensitive(false) // 设置大小写是否敏感
                    .withQuoting(Quoting.BACK_TICK) // 设置应用标识,mysql是``
                    .withQuotedCasing(Casing.TO_UPPER) // Quoting策略,不变,变大写或变小写
                    .withUnquotedCasing(Casing.TO_UPPER) // 标识符没有被Quoting后的策略
            )
            .defaultSchema(schema)
            .build()
        val planner = Frameworks.getPlanner(config)
        val sqlNode = planner.parse(sql)
        planner.validate(sqlNode)
        val relNode = planner.rel(sqlNode)
        println(relNode.rel.explain())
    }
}
