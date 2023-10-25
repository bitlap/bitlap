/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.mdm.model

import scala.jdk.CollectionConverters._

import org.bitlap.common.BitlapIterator
import org.bitlap.core.extension._
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
    val rs = this.rows.asScala.map { row =>
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
