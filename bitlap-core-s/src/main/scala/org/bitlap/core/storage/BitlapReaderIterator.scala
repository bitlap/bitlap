/** Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.storage

import scala.jdk.CollectionConverters.*

import org.bitlap.common.BitlapBatchIterator
import org.bitlap.common.utils.PreConditions

import java.util.List as JList



/** Batch iterator for metric row
 */
open class BitlapReaderIterator[R](private val reader: BitlapReader[R], private val limit: Int)
    extends BitlapBatchIterator[R]() {

  @volatile
  private var closed = false

  override def nextBatch(): JList[R] = {
    this.checkOpen()
    return this.reader.read(limit).asJava
  }

  override def hasNext(): Boolean = {
    this.checkOpen()
    val has = this.reader.hasNext || super.hasNext
    // auto close
    if (!has) {
      this.close()
    }
    return has
  }

  override def next(): R = {
    this.checkOpen()
    return super.next()
  }

  override def close(): Unit = {
    if (closed) {
      return
    }
    closed = true
    scala.util.Using.resource(this.reader) { _ => }
  }

  private def checkOpen(): Unit = {
    PreConditions.checkExpression(!closed, "", "BitlapReaderIterator has been closed.")
  }
}
