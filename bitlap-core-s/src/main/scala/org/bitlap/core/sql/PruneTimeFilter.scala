/** Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.sql

import java.io.Serializable

import scala.collection.mutable.ListBuffer

/** Prune time filter to push down
 */
type TimeFilterFun = (Long) => Boolean

case class PruneTimeFilterExpr(val name: String, val func: TimeFilterFun, val expr: String)

class PruneTimeFilter extends Serializable {

  private val conditions = ListBuffer[PruneTimeFilterExpr]()

  def add(name: String, func: TimeFilterFun, expr: String): PruneTimeFilter = {
    this.conditions += PruneTimeFilterExpr(name, func, expr)
    this
  }

  /** merge conditions into one
   */
  def mergeCondition(): TimeFilterFun = {
    return i => {
      this.conditions.map(_.func).forall(_(i))
    }
  }

  override def toString: String = {
    return this.conditions.map(_.expr).toString
  }
}
