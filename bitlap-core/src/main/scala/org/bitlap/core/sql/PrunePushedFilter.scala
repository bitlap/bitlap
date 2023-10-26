/*
 * Copyright 2020-2023 IceMimosa, jxnu-liguobin and the Bitlap Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
