/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.storage

import scala.jdk.CollectionConverters.*

import org.bitlap.common.utils.PreConditions
import org.bitlap.core.sql.FilterOp
import org.bitlap.core.sql.PrunePushedFilter
import org.bitlap.core.sql.PrunePushedFilterExpr

import org.apache.avro.Schema
import org.apache.parquet.filter2.predicate.FilterApi
import org.apache.parquet.filter2.predicate.FilterPredicate
import org.apache.parquet.io.api.Binary

/** Desc: bitlap reader utils
 */
object BitlapReaders {

  /** make avro schema with projections
   */
  def makeAvroSchema(schema: Schema, projections: List[String]): Schema = {
    PreConditions.checkExpression(projections.nonEmpty, "", "Make avro schema projections cannot be empty.")
    val fields = schema.getFields.asScala
      .filter(f => projections.contains(f.name()))
      .map(f => Schema.Field(f.name(), f.schema(), f.doc(), f.defaultVal(), f.order()))
      .asJava
    Schema.createRecord(schema.getName, schema.getDoc, schema.getNamespace, schema.isError, fields)
  }

  /** make parquet eq/in filter
   */
  def makeParquetFilter(column: String, value: String): FilterPredicate = {
    this.makeParquetFilterAnd(List(column -> List(value)))
  }

  def makeParquetFilter(column: String, value: List[String]): FilterPredicate = {
    this.makeParquetFilterAnd(List(column -> value))
  }

  def makeParquetFilterAnd(pairs: List[(String, List[String])]): FilterPredicate = {
    PreConditions.checkExpression(pairs.nonEmpty, "", "Make parquet filters cannot be empty.")
    val base = this.makeParquetColumnFilter(pairs.head._1, pairs.head._2)
    if (pairs.size == 1) {
      return base
    }
    val expr = pairs.drop(1).foldLeft(base) { case (p1, p2) =>
      FilterApi.and(
        p1,
        this.makeParquetColumnFilter(p2._1, p2._2)
      )
    }
    expr
  }

  private def makeParquetColumnFilter(column: String, values: List[String]): FilterPredicate = {
    if (values.size == 1) {
      return FilterApi.eq(FilterApi.binaryColumn(column), Binary.fromString(values.head))
    }
    FilterApi.in(FilterApi.binaryColumn(column), values.map(Binary.fromString).toSet.asJava)
  }

  /** make parquet filter from [PrunePushedFilter]
   */
  def makeParquetFilterFromPrunePushedFilter(filter: PrunePushedFilter, column: String): FilterPredicate = {
    val conditions = filter.getConditions
    if (conditions.isEmpty) {
      return null
    }
    var expr = this.makeParquetFilterFromPrunePushedFilter(conditions.head, column)
    conditions.drop(1).foreach { it =>
      expr = FilterApi.and(
        expr,
        this.makeParquetFilterFromPrunePushedFilter(it, column)
      )
    }
    expr
  }

  private def makeParquetFilterFromPrunePushedFilter(expr: PrunePushedFilterExpr, column: String): FilterPredicate = {
    val columnExpr = FilterApi.binaryColumn(column)
    expr.op match {
      case FilterOp.EQUALS =>
        if (expr.values.size == 1) {
          FilterApi.eq(columnExpr, Binary.fromString(expr.values.head))
        } else {
          FilterApi.in(columnExpr, expr.values.map(Binary.fromString).toSet.asJava)
        }

      case FilterOp.NOT_EQUALS =>
        if (expr.values.size == 1) {
          FilterApi.notEq(columnExpr, Binary.fromString(expr.values.head))
        } else {
          FilterApi.notIn(
            columnExpr,
            expr.values.map(Binary.fromString).toSet.asJava
          )
        }

      case FilterOp.GREATER_THAN =>
        FilterApi.gt(columnExpr, Binary.fromString(expr.values.head))

      case FilterOp.GREATER_EQUALS_THAN =>
        FilterApi.gtEq(columnExpr, Binary.fromString(expr.values.head))

      case FilterOp.LESS_THAN =>
        FilterApi.lt(columnExpr, Binary.fromString(expr.values.head))

      case FilterOp.LESS_EQUALS_THAN =>
        FilterApi.ltEq(columnExpr, Binary.fromString(expr.values.head))

      case FilterOp.OPEN =>
        FilterApi.and(
          FilterApi.gt(columnExpr, Binary.fromString(expr.values.head)),
          FilterApi.lt(columnExpr, Binary.fromString(expr.values.last))
        )

      case FilterOp.CLOSED =>
        FilterApi.and(
          FilterApi.gtEq(columnExpr, Binary.fromString(expr.values.head)),
          FilterApi.ltEq(columnExpr, Binary.fromString(expr.values.last))
        )

      case FilterOp.CLOSED_OPEN =>
        FilterApi.and(
          FilterApi.gtEq(columnExpr, Binary.fromString(expr.values.head)),
          FilterApi.lt(columnExpr, Binary.fromString(expr.values.last))
        )

      case FilterOp.OPEN_CLOSED =>
        FilterApi.and(
          FilterApi.gt(columnExpr, Binary.fromString(expr.values.head)),
          FilterApi.ltEq(columnExpr, Binary.fromString(expr.values.last))
        )
    }
  }
}
