package org.bitlap.core.sql

import com.google.protobuf.ByteString
import java.sql.Types
import org.apache.calcite.adapter.java.AbstractQueryableTable
import org.apache.calcite.avatica.util.Casing
import org.apache.calcite.avatica.util.Quoting
import org.apache.calcite.linq4j.QueryProvider
import org.apache.calcite.linq4j.Queryable
import org.apache.calcite.rel.type.RelDataType
import org.apache.calcite.rel.type.RelDataTypeFactory
import org.apache.calcite.schema.SchemaPlus
import org.apache.calcite.schema.impl.ScalarFunctionImpl
import org.apache.calcite.sql.BitlapSqlDdlNode
import org.apache.calcite.sql.SqlIdentifier
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.`fun`.SqlStdOperatorTable
import org.apache.calcite.sql.parser.SqlParser
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.sql.parser.bitlap.BitlapSqlParserImpl
import org.apache.calcite.sql.type.InferTypes
import org.apache.calcite.sql.type.OperandTypes
import org.apache.calcite.sql.type.ReturnTypes
import org.apache.calcite.sql.type.SqlTypeFamily
import org.apache.calcite.sql.type.SqlTypeName
import org.apache.calcite.sql.util.ListSqlOperatorTable
import org.apache.calcite.sql.util.SqlOperatorTables
import org.apache.calcite.sql.validate.SqlUserDefinedFunction
import org.apache.calcite.tools.Frameworks
import org.apache.calcite.tools.RelBuilder
import org.apache.calcite.tools.RelRunners
import org.bitlap.network.QueryResult
import org.bitlap.network.core.ColumnDesc
import org.bitlap.network.core.Row
import org.bitlap.network.core.RowSet
import org.bitlap.network.core.TableSchema
import org.bitlap.network.core.TypeId

/**
 * Desc:
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/8/6
 */
class QueryExecution(
    private val statement: String,
) {

    companion object {
        // TODO
        private val schema = Frameworks.createRootSchema(true).apply {
            add(
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
        }

        private val listSqlOperatorTable = ListSqlOperatorTable().apply {
            add(
                SqlUserDefinedFunction(
                    SqlIdentifier("hello", SqlParserPos.ZERO),
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.VARCHAR_2000,
                    InferTypes.FIRST_KNOWN,
                    OperandTypes.operandMetadata(
                        listOf(SqlTypeFamily.STRING),
                        { t -> t.builder().add("a", SqlTypeName.VARCHAR, 15).build().fieldList.map { it.type } }, { "a" }
                    ) { false },
                    ScalarFunctionImpl.create(Functions::class.java, "hello")
                )
            )
        }

        private val config = Frameworks.newConfigBuilder()
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
    }

    fun execute(): QueryResult {
        val planner = Frameworks.getPlanner(config)
        val sqlNode = planner.parse(statement)
        val rel = when (sqlNode) {
            is BitlapSqlDdlNode -> {
                val bbb = RelBuilder.create(config)
                sqlNode.rel(bbb)
            }
            else -> {
                planner.validate(sqlNode)
                val relNode = planner.rel(sqlNode)
//                println(relNode.rel.explain())
                relNode.rel
            }
        }
        val r = RelRunners.run(rel).executeQuery()
        // get schema
        val metaData = r.metaData
        val columns = (1..metaData.columnCount).map {
            val colName = metaData.getColumnName(it)
            val colType = when (metaData.getColumnType(it)) {
                Types.VARCHAR -> TypeId.B_TYPE_ID_STRING_TYPE
                Types.INTEGER -> TypeId.B_TYPE_ID_INT_TYPE
                Types.DOUBLE -> TypeId.B_TYPE_ID_DOUBLE_TYPE
                // TODO more
                else -> TypeId.B_TYPE_ID_UNSPECIFIED
            }
            ColumnDesc(colName, colType)
        }
        // get row set
        val rows = mutableListOf<Row>()
        while (r.next()) {
            val cols = (1..metaData.columnCount).map {
                when (metaData.getColumnType(it)) {
                    Types.VARCHAR -> ByteString.copyFromUtf8(r.getString(it))
                    // TODO more
                    else -> ByteString.copyFrom(r.getBytes(it))
                }
            }
            rows.add(Row(cols))
        }
        return QueryResult(TableSchema(columns), RowSet(rows))
    }
}
