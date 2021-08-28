package org.bitlap.core.sql

import com.google.common.collect.ImmutableList
import org.apache.calcite.adapter.java.AbstractQueryableTable
import org.apache.calcite.avatica.util.Casing
import org.apache.calcite.avatica.util.Quoting
import org.apache.calcite.linq4j.QueryProvider
import org.apache.calcite.linq4j.Queryable
import org.apache.calcite.rel.type.RelDataType
import org.apache.calcite.rel.type.RelDataTypeFactory
import org.apache.calcite.rel.type.RelDataTypeSystem
import org.apache.calcite.runtime.SqlFunctions
import org.apache.calcite.schema.SchemaPlus
import org.apache.calcite.schema.impl.ScalarFunctionImpl
import org.apache.calcite.sql.SqlIdentifier
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.`fun`.SqlStdOperatorTable
import org.apache.calcite.sql.parser.SqlParser
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.sql.parser.bitlap.BitlapSqlParserImpl
import org.apache.calcite.sql.type.InferTypes
import org.apache.calcite.sql.type.OperandTypes
import org.apache.calcite.sql.type.ReturnTypes
import org.apache.calcite.sql.type.SqlTypeFactoryImpl
import org.apache.calcite.sql.type.SqlTypeFamily
import org.apache.calcite.sql.type.SqlTypeName
import org.apache.calcite.sql.util.ChainedSqlOperatorTable
import org.apache.calcite.sql.util.ListSqlOperatorTable
import org.apache.calcite.sql.util.SqlOperatorTables
import org.apache.calcite.sql.validate.SqlUserDefinedFunction
import org.apache.calcite.tools.Frameworks
import org.apache.calcite.tools.RelRunners
import org.bitlap.core.sql.parser.SqlShowDataSources
import java.util.function.Function


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

        val listSqlOperatorTable = ListSqlOperatorTable()
        listSqlOperatorTable.add(SqlUserDefinedFunction(
            SqlIdentifier("hello", SqlParserPos.ZERO),
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.VARCHAR_2000,
            InferTypes.FIRST_KNOWN,
            OperandTypes.operandMetadata(listOf(SqlTypeFamily.STRING),
                { t -> t.builder().add("a", SqlTypeName.VARCHAR, 15).build().fieldList.map { it.type } }, { "a" }) { false },
            ScalarFunctionImpl.create(Functions::class.java, "hello")
        ))

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
            .operatorTable(SqlOperatorTables.chain(listSqlOperatorTable, SqlStdOperatorTable.instance()))
            .build()
        val planner = Frameworks.getPlanner(config)
        val sqlNode = planner.parse(sql)
        planner.validate(sqlNode)
        val relNode = planner.rel(sqlNode)
        println(relNode.rel.explain())

        val r = RelRunners.run(relNode.rel).executeQuery()
        while (r.next()) {
            println(r.getString(1))
        }
    }
}
