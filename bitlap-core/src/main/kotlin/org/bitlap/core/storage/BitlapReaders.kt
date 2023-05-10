package org.bitlap.core.storage

import org.apache.avro.Schema
import org.apache.parquet.filter2.predicate.FilterApi
import org.apache.parquet.filter2.predicate.FilterPredicate
import org.apache.parquet.io.api.Binary
import org.bitlap.common.utils.PreConditions
import org.bitlap.core.sql.FilterOp
import org.bitlap.core.sql.PrunePushedFilter
import org.bitlap.core.sql.PrunePushedFilterExpr

/**
 * Desc: bitlap reader utils
 */
object BitlapReaders {

    /**
     * make avro schema with projections
     */
    fun makeAvroSchema(schema: Schema, projections: List<String>): Schema {
        PreConditions.checkExpression(projections.isNotEmpty(), msg = "Make avro schema projections cannot be empty.")
        val fields = schema.fields
            .filter { projections.contains(it.name()) }
            .map { Schema.Field(it.name(), it.schema(), it.doc(), it.defaultVal(), it.order()) }
        return Schema.createRecord(schema.name, schema.doc, schema.namespace, schema.isError, fields)
    }

    /**
     * make parquet eq/in filter
     */
    fun makeParquetFilter(column: String, value: String): FilterPredicate {
        return this.makeParquetFilterAnd(listOf(column to listOf(value)))
    }

    fun makeParquetFilter(column: String, value: List<String>): FilterPredicate {
        return this.makeParquetFilterAnd(listOf(column to value))
    }

    fun makeParquetFilterAnd(pairs: List<Pair<String, List<String>>>): FilterPredicate {
        PreConditions.checkExpression(pairs.isNotEmpty(), msg = "Make parquet filters cannot be empty.")
        val base = this.makeParquetColumnFilter(pairs.first().first, pairs.first().second)
        if (pairs.size == 1) {
            return base
        }
        val expr = pairs.drop(1).fold(base) { p1, p2 ->
            FilterApi.and(
                p1,
                this.makeParquetColumnFilter(p2.first, p2.second)
            )
        }
        return expr
    }

    private fun makeParquetColumnFilter(column: String, values: List<String>): FilterPredicate {
        if (values.size == 1) {
            return FilterApi.eq(FilterApi.binaryColumn(column), Binary.fromString(values.first()))
        }
        return FilterApi.`in`(FilterApi.binaryColumn(column), values.map { Binary.fromString(it) }.toSet())
    }


    /**
     * make parquet filter from [PrunePushedFilter]
     */
    fun makeParquetFilterFromPrunePushedFilter(filter: PrunePushedFilter, column: String): FilterPredicate? {
        val conditions = filter.getConditions()
        if (conditions.isEmpty()) {
            return null
        }
        var expr = this.makeParquetFilterFromPrunePushedFilter(conditions.first(), column)
        conditions.drop(1).forEach {
            expr = FilterApi.and(
                expr,
                this.makeParquetFilterFromPrunePushedFilter(it, column)
            )
        }
        return expr
    }

    private fun makeParquetFilterFromPrunePushedFilter(expr: PrunePushedFilterExpr, column: String): FilterPredicate {
        val columnExpr = FilterApi.binaryColumn(column)
        return when (expr.op) {
            FilterOp.EQUALS -> {
                if (expr.values.size == 1) {
                    FilterApi.eq(columnExpr, Binary.fromString(expr.values.first()))
                } else {
                    FilterApi.`in`(columnExpr, expr.values.map { Binary.fromString(it) }.toSet())
                }
            }

            FilterOp.NOT_EQUALS ->
                if (expr.values.size == 1) {
                    FilterApi.notEq(columnExpr, Binary.fromString(expr.values.first()))
                } else {
                    FilterApi.notIn(
                        columnExpr,
                        expr.values.map { Binary.fromString(it) }.toSet()
                    )
                }

            FilterOp.GREATER_THAN ->
                FilterApi.gt(columnExpr, Binary.fromString(expr.values.first()))

            FilterOp.GREATER_EQUALS_THAN ->
                FilterApi.gtEq(columnExpr, Binary.fromString(expr.values.first()))

            FilterOp.LESS_THAN ->
                FilterApi.lt(columnExpr, Binary.fromString(expr.values.first()))

            FilterOp.LESS_EQUALS_THAN ->
                FilterApi.ltEq(columnExpr, Binary.fromString(expr.values.first()))

            FilterOp.OPEN ->
                FilterApi.and(
                    FilterApi.gt(columnExpr, Binary.fromString(expr.values.first())),
                    FilterApi.lt(columnExpr, Binary.fromString(expr.values.last())),
                )

            FilterOp.CLOSED ->
                FilterApi.and(
                    FilterApi.gtEq(columnExpr, Binary.fromString(expr.values.first())),
                    FilterApi.ltEq(columnExpr, Binary.fromString(expr.values.last())),
                )

            FilterOp.CLOSED_OPEN ->
                FilterApi.and(
                    FilterApi.gtEq(columnExpr, Binary.fromString(expr.values.first())),
                    FilterApi.lt(columnExpr, Binary.fromString(expr.values.last())),
                )

            FilterOp.OPEN_CLOSED ->
                FilterApi.and(
                    FilterApi.gt(columnExpr, Binary.fromString(expr.values.first())),
                    FilterApi.ltEq(columnExpr, Binary.fromString(expr.values.last())),
                )
        }
    }
}
