/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.sql

import java.io.Serializable

import scala.collection.mutable.ListBuffer

/** Prune other filter to push down
 */
type PushedFilterFun = (String) => Boolean

final case class PrunePushedFilterExpr(
  name: String,
  op: FilterOp,
  values: List[String],
  func: PushedFilterFun,
  expr: String)

class PrunePushedFilter extends Serializable {

  private val conditions = ListBuffer[PrunePushedFilterExpr]()

  def add(
    name: String,
    op: FilterOp,
    values: List[String],
    func: PushedFilterFun,
    expr: String
  ): PrunePushedFilter = {
    this.conditions += PrunePushedFilterExpr(name, op, values, func, expr)
    this
  }

  def filter(name: String): PrunePushedFilter = {
    val rs = this.conditions.filter(_.name == name)
    this.conditions.addAll(rs)
    this
  }

  def getNames: Set[String]                      = this.conditions.map(_.name).toSet
  def getConditions: List[PrunePushedFilterExpr] = this.conditions.toList

  override def toString: String = {
    this.conditions.map(_.expr).mkString("[", ",", "]")
  }
}
