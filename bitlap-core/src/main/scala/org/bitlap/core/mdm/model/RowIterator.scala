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
package org.bitlap.core.mdm.model

import org.bitlap.common.BitlapIterator
import org.bitlap.common.extension._
import org.bitlap.core.mdm.format.DataType

/** Iterator for [Row] with row type.
 */
class RowIterator(
  val rows: BitlapIterator[Row],
  val keyTypes: List[DataType],
  val valueTypes: List[DataType])
    extends BitlapIterator[Row] {

  private val dataTypes = keyTypes ++ valueTypes
  private val dtNameMap = dataTypes.groupBy(_.name).map(kv => kv._1 -> kv._2.head)

  override def hasNext: Boolean = {
    this.rows.hasNext
  }

  override def next(): Row = {
    this.rows.next()
  }

  /** get types for input strings.
   */
  def getTypes(names: List[String]): List[DataType] = {
    names.map(n => this.dtNameMap(n))
  }

  def getType(name: String): DataType = {
    this.dtNameMap(name)
  }

  /** get array result
   */
  def toRows(projections: List[String]): Iterable[Array[Any]] = {
    val pTypes = projections.map { it =>
      Option(this.dtNameMap(it)) match
        case Some(value) => value
        case None =>
          throw IllegalArgumentException(
            s"Input projections $projections contain a column that is not in current dataTypes $dataTypes."
          )
    }
    val rs = this.rows.map { row =>
      Array.fill[Any](pTypes.size)(null).also { arr =>
        pTypes.zipWithIndex.foreach { (c, i) =>
          arr(i) = row(c.idx)
        }
      }
    }
    Iterable.from(rs)
  }
}

object RowIterator {
  def empty(): RowIterator = RowIterator(BitlapIterator.empty(), List.empty[DataType], List.empty[DataType])
}
